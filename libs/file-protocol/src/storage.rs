//
// Copyright (C) 2018 Kubos Corporation
//
// Licensed under the Apache License, Version 2.0 (the "License")
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.
//

use crate::error::ProtocolError;
use blake2_rfc::blake2s::Blake2s;
use log::warn;
use serde_cbor::{de, to_vec, Value};
use std::fs::File;
use std::fs::Permissions;
use std::io::Seek;
use std::io::SeekFrom;
use std::io::{BufRead, BufReader, Read, Write};
use std::os::unix::fs::MetadataExt;
use std::os::unix::fs::PermissionsExt;
use std::path::Path;
use std::str;
use std::thread;
use std::time::Duration;
use std::{fmt::format, fs};
use time;

const HASH_SIZE: usize = 16;

// Save new chunk in a temporary storage file
pub fn store_chunk(prefix: &str, hash: &str, index: u32, data: &[u8]) -> Result<(), ProtocolError> {
    let file_name = format!("{}", index);
    let storage_path = Path::new(&format!("{}/storage", prefix))
        .join(hash)
        .join(file_name);

    if let Some(parent) = &storage_path.parent() {
        fs::create_dir_all(parent).map_err(|err| ProtocolError::StorageError {
            action: format!("create storage directory {:?}", storage_path),
            err,
        })?;
    }

    let mut file = File::create(&storage_path).map_err(|err| ProtocolError::StorageError {
        action: "create storage file".to_owned(),
        err,
    })?;

    file.write_all(data)
        .map_err(|err| ProtocolError::StorageError {
            action: "write chunk".to_owned(),
            err,
        })?;

    Ok(())
}

#[derive(serde::Serialize, serde::Deserialize)]
struct Meta {
    num_chunks: u32,
    chunk_size: Option<u64>,
    file_path: Option<String>,
}

pub fn store_meta(
    prefix: &str,
    hash: &str,
    num_chunks: u32,
    chunk_size: Option<u64>,
    file_path: Option<&str>,
) -> Result<(), ProtocolError> {
    let data = Meta {
        num_chunks,
        chunk_size,
        file_path: file_path.map(|f| f.to_owned()),
    };

    let vec = to_vec(&data)?;

    let file_dir = Path::new(&format!("{}/storage", prefix)).join(hash);
    // Make sure the directory exists
    fs::create_dir_all(file_dir.clone()).map_err(|err| ProtocolError::StorageError {
        action: "create temp storage directory".to_owned(),
        err,
    })?;

    let meta_path = file_dir.join("meta");
    let temp_path = file_dir.join(".meta.tmp");

    File::create(&temp_path)
        .map_err(|err| ProtocolError::StorageError {
            action: format!("create/open {:?} for writing", temp_path),
            err,
        })?
        .write_all(&vec)
        .map_err(|err| ProtocolError::StorageError {
            action: format!("write metadata to {:?}", temp_path),
            err,
        })?;

    fs::rename(temp_path.clone(), meta_path.clone()).map_err(|err| {
        ProtocolError::StorageError {
            action: format!("rename {:?} to {:?}", temp_path, meta_path),
            err,
        }
    })?;

    Ok(())
}

// Load a chunk from its temporary storage file
pub fn load_chunk(prefix: &str, hash: &str, index: u32) -> Result<Vec<u8>, ProtocolError> {
    let mut data = vec![];
    if let (_, Some(chunk_size), Some(path)) = load_meta(prefix, hash)? {
        // let path = Path::new(&format!("{}/storage", prefix))
        //     .join(hash)
        //     .join(format!("{}", index));

        let mut file = File::open(&path).map_err(|err| ProtocolError::StorageError {
            action: format!("open chunk file {}", index),
            err,
        })?;

        file.seek(SeekFrom::Start(chunk_size * index as u64))
            .map_err(|err| ProtocolError::StorageError {
                action: format!("seek to chunk in file {}", &path),
                err,
            })?;

        file.take(chunk_size)
            .read_to_end(&mut data)
            .map_err(|err| ProtocolError::StorageError {
                action: format!("read chunk file {}", index),
                err,
            })?;
    } else {
        let path = Path::new(&format!("{}/storage", prefix))
            .join(hash)
            .join(format!("{}", index));

        File::open(path)
            .map_err(|err| ProtocolError::StorageError {
                action: format!("open chunk file {}", index),
                err,
            })?
            .read_to_end(&mut data)
            .map_err(|err| ProtocolError::StorageError {
                action: format!("read chunk file {}", index),
                err,
            })?;
    }
    Ok(data)
}

// Load number of chunks in file from metadata
pub fn load_meta(
    prefix: &str,
    hash: &str,
) -> Result<(u32, Option<u64>, Option<String>), ProtocolError> {
    let mut data = vec![];
    let meta_path = Path::new(&format!("{}/storage", prefix))
        .join(hash)
        .join("meta");

    File::open(meta_path)
        .map_err(|err| ProtocolError::StorageError {
            action: format!("open {} metadata file", hash),
            err,
        })?
        .read_to_end(&mut data)
        .map_err(|err| ProtocolError::StorageError {
            action: format!("read {} metadata file", hash),
            err,
        })?;

    let metadata: Meta = de::from_slice(&data).map_err(|err| {
        ProtocolError::StorageParseError(format!("Unable to parse metadata for {}: {}", hash, err))
    })?;

    Ok((metadata.num_chunks, metadata.chunk_size, metadata.file_path))
}

// Check if all of a files chunks are present in the temporary directory
pub fn validate_file(
    prefix: &str,
    hash: &str,
    num_chunks: Option<u32>,
) -> Result<(bool, Vec<u32>), ProtocolError> {
    let num_chunks = if let Some(num) = num_chunks {
        store_meta(prefix, hash, num, None, None)?;
        num
    } else {
        let (num, ..) = load_meta(prefix, hash)?;
        num
    };

    let mut missing_ranges: Vec<u32> = vec![];

    let hash_path = Path::new(&format!("{}/storage", prefix)).join(hash);

    let mut prev_entry: i32 = -1;

    let entries = fs::read_dir(hash_path.clone()).map_err(|err| ProtocolError::StorageError {
        action: format!("read {:?} directory", hash_path),
        err,
    })?;

    let mut converted_entries: Vec<i32> = entries
        .filter_map(|entry| entry.ok())
        .filter_map(|entry| {
            match entry
                .file_name()
                .into_string()
                .map_err(|err| {
                    ProtocolError::StorageParseError(format!(
                        "Failed to parse file name: {:?}",
                        err
                    ))
                })
                .and_then(|val| {
                    val.parse::<i32>().map_err(|err| {
                        ProtocolError::StorageParseError(format!(
                            "Failed to parse chunk_number {:?}",
                            err
                        ))
                    })
                }) {
                Ok(num) => Some(num),
                _ => None,
            }
        })
        .collect();

    converted_entries.sort();

    let mut max_entries = 186;
    for &entry_num in converted_entries.iter() {
        //println!("checking {} vs {}", entry_num, prev_entry);
        // Check for non-sequential dir entries to detect missing chunk ranges
        if entry_num - prev_entry > 1 {
            // Add start of range (inclusive)
            missing_ranges.push((prev_entry + 1) as u32);
            // Add end of range (non-inclusive)
            missing_ranges.push(entry_num as u32);

            max_entries -= 1;

            if max_entries == 0 {
                break;
            }
        }

        prev_entry = entry_num;
    }

    // Check for a trailing range
    // Ex. Last known chunk is 5, but there are 10 chunks.
    //     We will already have added '6', so we need to add '10'
    //     to close it out.
    if max_entries != 0 && (num_chunks as i32) - prev_entry != 1 {
        // Add start of range
        missing_ranges.push((prev_entry + 1) as u32);
        // Add end of range
        missing_ranges.push(num_chunks as u32);
    }

    Ok((missing_ranges.is_empty(), missing_ranges))
}

/// Create temporary folder for chunks
/// Stream copy file from mutable space to immutable space
/// Move folder to hash of contents
/// Import file into chunked storage for transfer
pub fn initialize_file(
    prefix: &str,
    source_path: &str,
    transfer_chunk_size: usize,
    hash_chunk_size: usize,
) -> Result<(String, u32, u32), ProtocolError> {
    let storage_path = format!("{}/storage", prefix);

    // Confirm file exists
    let metadata = fs::metadata(source_path).map_err(|err| ProtocolError::StorageError {
        action: format!("stat file {}", source_path),
        err,
    })?;

    // Create necessary storage directory
    fs::create_dir_all(&storage_path).map_err(|err| ProtocolError::StorageError {
        action: format!("create dir {}", storage_path),
        err,
    })?;

    // Calculate hash of temp file
    let hash = calc_file_hash(&source_path, hash_chunk_size)?;

    let file_size = metadata.len() as u64;
    let index = (file_size / transfer_chunk_size as u64) as u32
        + ((file_size % transfer_chunk_size as u64) > 0) as u32;

    store_meta(
        prefix,
        &hash,
        index,
        Some(transfer_chunk_size as u64),
        Some(&source_path),
    )?;

    if let Ok(meta) = fs::metadata(source_path) {
        Ok((hash, index, meta.mode()))
    } else {
        Ok((hash, index, 0o644))
    }
}

// Export received chunks into final file and verify correct file hash
pub fn finalize_file(
    prefix: &str,
    hash: &str,
    target_path: &str,
    mode: Option<u32>,
    hash_chunk_size: usize,
) -> Result<(), ProtocolError> {
    // Double check that all the chunks of the file are present
    let (result, _) = validate_file(prefix, hash, None)?;

    if !result {
        return Err(ProtocolError::FinalizeError {
            cause: "file missing chunks".to_owned(),
        });
    }

    // Get the total number of chunks we're saving
    let (num_chunks, _, _) = load_meta(prefix, hash)?;

    // Q: Do we want to create the parent directories if they don't exist?
    let mut file = File::create(target_path).map_err(|err| ProtocolError::StorageError {
        action: format!("create/open file for writing {}", target_path),
        err,
    })?;

    // Set exported file's mode
    if let Some(mode_val) = mode {
        file.set_permissions(Permissions::from_mode(mode_val))
            .map_err(|err| ProtocolError::StorageError {
                action: "set target file's mode".to_owned(),
                err,
            })?;
    }

    // Iterate through chunks and reassemble file
    let mut load_chunk_err = None;
    for chunk_num in 0..num_chunks {
        let chunk = match load_chunk(prefix, hash, chunk_num) {
            Ok(c) => c,
            Err(e) => {
                warn!(
                    "Error encountered loading chunk {}, deleting : {}",
                    chunk_num, e
                );
                delete_chunk(prefix, hash, chunk_num)?;
                load_chunk_err = Some(e);
                continue;
            }
        };

        // Write the chunk to the destination file
        file.write_all(&chunk)
            .map_err(|err| ProtocolError::StorageError {
                action: format!("write chunk {}", chunk_num),
                err,
            })?;
    }

    if let Some(e) = load_chunk_err {
        return Err(e);
    }

    // Calculate hash of exported file
    let calc_hash_str = calc_file_hash(&target_path, hash_chunk_size)?;

    // Final determination if file was correctly received and assembled
    if calc_hash_str == hash {
        Ok(())
    } else {
        // If the hash doesn't match then we start over
        delete_file(&prefix, &hash)?;
        Err(ProtocolError::HashMismatch)
    }
}

pub fn delete_chunk(prefix: &str, hash: &str, index: u32) -> Result<(), ProtocolError> {
    let path = Path::new(&format!("{}/storage", prefix))
        .join(hash)
        .join(format!("{}", index));

    fs::remove_file(path).map_err(|err| ProtocolError::StorageError {
        action: format!("deleting chunk file {}", index),
        err,
    })?;

    Ok(())
}

pub fn delete_file(prefix: &str, hash: &str) -> Result<(), ProtocolError> {
    let path = Path::new(&format!("{}/storage", prefix)).join(hash);
    fs::remove_dir_all(path).map_err(|err| ProtocolError::StorageError {
        action: format!("deleting file {}", hash),
        err,
    })?;

    Ok(())
}

pub fn delete_storage(prefix: &str) -> Result<(), ProtocolError> {
    let path = prefix.to_owned();
    let path = Path::new(&path);
    fs::remove_dir_all(path).map_err(|err| ProtocolError::StorageError {
        action: format!("deleting path {:?}", path),
        err,
    })?;

    Ok(())
}

/// Calculate the blake2s hash for a file at given path
fn calc_file_hash(path: &str, hash_chunk_size: usize) -> Result<String, ProtocolError> {
    let mut hasher = Blake2s::new(HASH_SIZE);
    let input = File::open(&path).map_err(|err| ProtocolError::StorageError {
        action: format!("open {:?}", path),
        err,
    })?;
    let mut reader = BufReader::with_capacity(hash_chunk_size * 8, input);

    // Need to bring in blake2fs here to create hash
    loop {
        let length = {
            let chunk = reader
                .fill_buf()
                .map_err(|err| ProtocolError::StorageError {
                    action: "read chunk from source".to_owned(),
                    err,
                })?;
            if chunk.is_empty() {
                break;
            }
            hasher.update(&chunk);
            chunk.len()
        };
        reader.consume(length);
        // thread::sleep(Duration::from_millis(2));
    }

    Ok(hasher
        .finalize()
        .as_bytes()
        .iter()
        .map(|val| format!("{:02x}", val))
        .collect::<String>())
}
