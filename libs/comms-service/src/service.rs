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
// Contributed by: William Greer (wgreer184@gmail.com) and Sam Justice (sam.justice1@gmail.com)
//

use crate::config::*;
use crate::errors::*;
use crate::packet::{LinkPacket, PayloadType};
use crate::telemetry::*;
use log::info;
use std::fmt::Debug;
use std::net::{Ipv4Addr, UdpSocket};
use std::str::FromStr;
use std::sync::atomic::AtomicU32;
use std::sync::atomic::Ordering;
use std::sync::mpsc;
use std::sync::mpsc::SendError;
use std::sync::{Arc, Mutex};
use std::thread;

/// Type definition for a "read" function pointer.
pub type ReadFn<Connection> = dyn Fn(&Connection) -> CommsResult<Vec<u8>> + Send + Sync + 'static;
/// Type definition for a "write" function pointer.
pub type WriteFn<Connection> =
    dyn Fn(&Connection, &[u8]) -> CommsResult<()> + Send + Sync + 'static;

/// Struct that holds configuration data to allow users to set up a Communication Service.
#[derive(Clone)]
pub struct CommsControlBlock<ReadConnection: Clone, WriteConnection: Clone> {
    /// Function pointer to a function that defines how to read from a gateway.
    pub read: Option<Arc<ReadFn<ReadConnection>>>,
    /// Function pointers to functions that define methods for writing data over a gateway.
    pub write: Vec<Arc<WriteFn<WriteConnection>>>,
    /// Gateway connection to read from.
    pub read_conn: ReadConnection,
    /// Gateway connection to write to.
    pub write_conn: WriteConnection,
    /// Maximum number of concurrent message handlers allowed.
    pub max_num_handlers: u16,
    /// Timeout for the completion of GraphQL operations within message handlers (in milliseconds).
    pub read_timeout: u64,
    /// Timeout for the completion of GraphQL operations within message handlers (in milliseconds).
    pub write_timeout: u64,
    /// IP address of the computer that is running the communication service.
    pub ip: Ipv4Addr,
    /// Optional list of ports used by downlink endpoints that send messages to the ground.
    /// Each port in the list will be used by one downlink endpoint.
    pub downlink_ports: Option<Vec<DownlinkPort>>,
}

impl<ReadConnection: Clone + Debug, WriteConnection: Clone + Debug> Debug
    for CommsControlBlock<ReadConnection, WriteConnection>
{
    fn fmt(&self, f: &mut ::std::fmt::Formatter) -> ::std::fmt::Result {
        let read = if self.read.is_some() {
            "Some(fn)"
        } else {
            "None"
        };

        let mut write = vec![];

        if !self.write.is_empty() {
            for _n in 0..self.write.len() {
                write.push("Fn");
            }
        }

        write!(
            f,
            "CommsControlBlock {{ read: {}, write: {:?}, read_conn: {:?}, write_conn: {:?},
            max_num_handlers: {:?}, timeout: {:?}:{:?}, ip: {:?}, downlink_ports: {:?} }}",
            read,
            write,
            self.read_conn,
            self.write_conn,
            self.max_num_handlers,
            self.read_timeout,
            self.write_timeout,
            self.ip,
            self.downlink_ports,
        )
    }
}

impl<ReadConnection: Clone, WriteConnection: Clone>
    CommsControlBlock<ReadConnection, WriteConnection>
{
    /// Creates a new instance of the CommsControlBlock
    pub fn new(
        read: Option<Arc<ReadFn<ReadConnection>>>,
        write: Vec<Arc<WriteFn<WriteConnection>>>,
        read_conn: ReadConnection,
        write_conn: WriteConnection,
        config: CommsConfig,
    ) -> CommsResult<Self> {
        if write.is_empty() {
            return Err(
                CommsServiceError::ConfigError("No `write` function provided".to_owned()).into(),
            );
        }

        if let Some(ports) = config.clone().downlink_ports {
            if write.len() != ports.len() {
                return Err(CommsServiceError::ConfigError(
                    "There must be a unique write function for each downlink port".to_owned(),
                )
                .into());
            }
        }

        Ok(CommsControlBlock {
            read,
            write,
            read_conn,
            write_conn,
            max_num_handlers: config.max_num_handlers.unwrap_or(DEFAULT_MAX_HANDLERS),
            read_timeout: config.read_timeout.unwrap_or(DEFAULT_TIMEOUT),
            write_timeout: config.write_timeout.unwrap_or(DEFAULT_TIMEOUT),
            ip: Ipv4Addr::from_str(&config.ip)?,
            downlink_ports: config.downlink_ports,
        })
    }
}

/// Struct that enables users to start the Communication Service.
pub struct CommsService;

impl CommsService {
    /// Starts an instance of the Communication Service and its associated background threads.
    pub fn start<
        ReadConnection: Clone + Send + 'static,
        WriteConnection: Clone + Send + 'static,
        Packet: LinkPacket + Send + 'static,
    >(
        control: CommsControlBlock<ReadConnection, WriteConnection>,
        telem: &Arc<Mutex<CommsTelemetry>>,
    ) -> CommsResult<()> {
        // If desired, spawn a read thread
        if control.read.is_some() {
            let telem_ref = telem.clone();
            let control_ref = control.clone();
            thread::Builder::new()
                .stack_size(16 * 1024)
                .spawn(move || {
                    read_thread::<ReadConnection, WriteConnection, Packet>(control_ref, &telem_ref)
                })
                .unwrap();
        }

        // For each provided `write()` function, spawn a downlink endpoint thread.
        if let Some(ports) = control.downlink_ports {
            for (_, (port, write)) in ports.iter().zip(control.write.iter()).enumerate() {
                let telem_ref = telem.clone();
                let port_ref = port.clone();
                let conn_ref = control.write_conn.clone();
                let write_ref = write.clone();
                let ip = control.ip;
                thread::Builder::new()
                    .stack_size(16 * 1024)
                    .spawn(move || {
                        downlink_endpoint::<ReadConnection, WriteConnection, Packet>(
                            &telem_ref, port_ref, conn_ref, &write_ref, ip,
                        );
                    })
                    .unwrap();
            }
        }

        info!("Communication service started");
        Ok(())
    }
}

// This thread reads from a gateway and passes received messages to message handlers.
fn read_thread<
    ReadConnection: Clone + Send + 'static,
    WriteConnection: Clone + Send + 'static,
    Packet: LinkPacket + Send + 'static,
>(
    comms: CommsControlBlock<ReadConnection, WriteConnection>,
    data: &Arc<Mutex<CommsTelemetry>>,
) {
    // Take reader from control block.
    let read = comms.read.unwrap();

    // Initiate counter for handlers
    let num_handlers: Arc<Mutex<u16>> = Arc::new(Mutex::new(0));

    loop {
        // Read bytes from the radio.
        let bytes = match (read)(&comms.read_conn.clone()) {
            Ok(bytes) => bytes,
            Err(e) => {
                log_error(&data, e.to_string()).unwrap();
                continue;
            }
        };

        // Create a link packet from the received information.
        let packet = match Packet::parse(&bytes) {
            Ok(packet) => packet,
            Err(e) => {
                log_telemetry(&data, &TelemType::UpFailed).unwrap();
                log_error(&data, CommsServiceError::HeaderParsing.to_string()).unwrap();
                error!("Failed to parse packet header {}", e);
                continue;
            }
        };

        // Validate the link packet
        if !packet.validate() {
            log_telemetry(&data, &TelemType::UpFailed).unwrap();
            log_error(&data, CommsServiceError::InvalidChecksum.to_string()).unwrap();
            error!("Packet checksum failed");
            continue;
        }

        // Update number of packets up.
        log_telemetry(&data, &TelemType::Up).unwrap();
        // info!("Packet successfully uplinked");

        // Check link type for appropriate message handling path
        match packet.payload_type() {
            PayloadType::Unknown(value) => {
                log_error(
                    &data,
                    CommsServiceError::UnknownPayloadType(value).to_string(),
                )
                .unwrap();
                error!("Unknown payload type encountered: {}", value);
            }
            PayloadType::UDP => {
                let sat_ref = comms.ip;
                let data_ref = data.clone();

                //                 thread::Builder::new()
                //                     .stack_size(16 * 1024)
                //                     .spawn(move ||
                match handle_udp_passthrough(packet, sat_ref) {
                    Ok(_) => {
                        log_telemetry(&data_ref, &TelemType::Down).unwrap();
                        // info!("UDP Packet successfully uplinked");
                    }
                    Err(e) => {
                        log_telemetry(&data_ref, &TelemType::DownFailed).unwrap();
                        log_error(&data_ref, e.to_string()).unwrap();
                        error!("UDP packet failed to uplink: {}", e.to_string());
                    }
                }
                //                     })
                //                     .unwrap();
            }
            PayloadType::GraphQL => {
                debug!("Received GraphQL Packet");
                if let Ok(mut num_handlers) = num_handlers.lock() {
                    if *num_handlers >= comms.max_num_handlers {
                        log_error(&data, CommsServiceError::NoAvailablePorts.to_string()).unwrap();
                        error!("No message handler ports available");
                        continue;
                    } else {
                        *num_handlers += 1;
                    }
                }

                // Spawn new message handler.
                let conn_ref = comms.write_conn.clone();
                let write_ref = comms.write[0].clone();
                let data_ref = data.clone();
                let sat_ref = comms.ip;
                let read_time_ref = comms.read_timeout;
                let write_time_ref = comms.write_timeout;
                let num_handlers_ref = num_handlers.clone();
                thread::Builder::new()
                    .stack_size(80 * 1024)
                    .spawn(move || {
                        let res = handle_graphql_request(
                            conn_ref,
                            &write_ref,
                            packet,
                            read_time_ref,
                            write_time_ref,
                            sat_ref,
                        );

                        if let Ok(mut num_handlers) = num_handlers_ref.lock() {
                            *num_handlers -= 1;
                        }

                        match res {
                            Ok(_) => {
                                log_telemetry(&data_ref, &TelemType::Down).unwrap();
                                // info!("GraphQL Packet successfully downlinked");
                            }
                            Err(e) => {
                                log_telemetry(&data_ref, &TelemType::DownFailed).unwrap();
                                log_error(&data_ref, e.to_string()).unwrap();
                                error!("GraphQL packet failed to downlink: {}", e.to_string());
                            }
                        }
                    })
                    .unwrap();
            }
            PayloadType::UDPDlStream => {
                if let Ok(mut num_handlers) = num_handlers.lock() {
                    if *num_handlers >= comms.max_num_handlers {
                        log_error(&data, CommsServiceError::NoAvailablePorts.to_string()).unwrap();
                        error!("No message handler ports available");
                        continue;
                    } else {
                        *num_handlers += 1;
                    }
                }

                // Spawn new message handler.
                let conn_ref = comms.write_conn.clone();
                let write_ref = comms.write[0].clone();
                let data_ref = data.clone();
                let sat_ref = comms.ip;
                let read_time_ref = comms.read_timeout * 10;
                let write_time_ref = comms.write_timeout * 10;
                let num_handlers_ref = num_handlers.clone();
                thread::Builder::new()
                    .stack_size(16 * 1024)
                    .spawn(move || {
                        let res = handle_udp_dl_stream_request(
                            conn_ref,
                            &write_ref,
                            packet,
                            read_time_ref,
                            write_time_ref,
                            sat_ref,
                        );

                        if let Ok(mut num_handlers) = num_handlers_ref.lock() {
                            *num_handlers -= 1;
                        }

                        match res {
                            Ok(_) => {
                                log_telemetry(&data_ref, &TelemType::Down).unwrap();
                                // info!("UDP DL Stream Completed");
                            }
                            Err(e) => {
                                log_telemetry(&data_ref, &TelemType::DownFailed).unwrap();
                                log_error(&data_ref, e.to_string()).unwrap();
                                error!("UDP Dl Stream Error: {}", e.to_string());
                            }
                        }
                    })
                    .unwrap();
            }
        }
    }
}

// This thread sends a query/mutation to its intended destination and waits for a response.
// The thread then writes the response to the gateway.
#[allow(clippy::boxed_local)]
fn handle_graphql_request<WriteConnection: Clone, Packet: LinkPacket>(
    write_conn: WriteConnection,
    write: &Arc<WriteFn<WriteConnection>>,
    message: Box<Packet>,
    read_timeout: u64,
    write_timeout: u64,
    sat_ip: Ipv4Addr,
) -> Result<(), String> {
    use std::time::Duration;

    let socket = UdpSocket::bind((sat_ip, 0)).map_err(|e| e.to_string())?;

    socket
        .set_read_timeout(Some(Duration::from_millis(read_timeout)))
        .map_err(|e| e.to_string())?;

    socket
        .set_write_timeout(Some(Duration::from_millis(write_timeout)))
        .map_err(|e| e.to_string())?;

    socket
        .send_to(&message.payload(), (sat_ip, message.destination()))
        .map_err(|e| e.to_string())?;
    debug!("Sent GraphQL Request to {}", message.destination());

    let mut buf = [0; 64 * 1024];

    let (size, _addr) = socket.recv_from(&mut buf).map_err(|e| e.to_string())?;
    debug!("Received GraphQL Response from {}", message.destination());

    // Take received message and wrap it in a LinkPacket
    let packet = Packet::build(message.command_id(), PayloadType::GraphQL, 0, &buf[0..size])
        .and_then(|packet| packet.to_bytes())
        .map_err(|e| e.to_string())?;

    // Write packet to the gateway
    write(&write_conn.clone(), &packet).map_err(|e| e.to_string())?;
    debug!("Downlinked GraphQL Response from {}", message.destination());

    Ok(())
}

#[allow(clippy::boxed_local)]
fn handle_udp_dl_stream_request<WriteConnection: Clone, Packet: LinkPacket>(
    write_conn: WriteConnection,
    write: &Arc<WriteFn<WriteConnection>>,
    message: Box<Packet>,
    read_timeout: u64,
    write_timeout: u64,
    sat_ip: Ipv4Addr,
) -> Result<(), String> {
    use std::time::Duration;

    let socket = UdpSocket::bind((sat_ip, 0)).map_err(|e| e.to_string())?;

    socket
        .set_read_timeout(Some(Duration::from_millis(read_timeout)))
        .map_err(|e| e.to_string())?;

    socket
        .set_write_timeout(Some(Duration::from_millis(write_timeout)))
        .map_err(|e| e.to_string())?;

    socket
        .send_to(&message.payload(), (sat_ip, message.destination()))
        .map_err(|e| e.to_string())?;

    let mut buf = [0; 16 * 1024];

    while let Ok((size, _addr)) = socket.recv_from(&mut buf) {
        // Take received message and wrap it in a LinkPacket
        let packet = Packet::build(
            message.command_id(),
            PayloadType::UDPDlStream,
            0,
            &buf[0..size],
        )
        .and_then(|packet| packet.to_bytes())
        .map_err(|e| e.to_string())?;

        // Write packet to the gateway
        write(&write_conn.clone(), &packet).map_err(|e| e.to_string())?;
    }

    Ok(())
}

// This function takes a Packet with PayloadType::UDP and sends the payload over a
// UdpSocket to the specified destination.
#[allow(clippy::boxed_local)]
fn handle_udp_passthrough<Packet: LinkPacket>(
    message: Box<Packet>,
    sat_ip: Ipv4Addr,
) -> Result<(), String> {
    let socket = UdpSocket::bind((sat_ip, 0)).map_err(|e| e.to_string())?;

    socket
        .send_to(&message.payload(), (sat_ip, message.destination()))
        .map_err(|e| e.to_string())
        .map(|_c| ())
}

// This thread reads indefinitely from a UDP socket, creating link packets from
// the UDP packet payload and then writes the link packets to a gateway.
fn downlink_endpoint<ReadConnection: Clone, WriteConnection: Clone, Packet: LinkPacket>(
    data: &Arc<Mutex<CommsTelemetry>>,
    port: DownlinkPort,
    write_conn: WriteConnection,
    write: &Arc<WriteFn<WriteConnection>>,
    sat_ip: Ipv4Addr,
) {
    // Bind the downlink endpoint to a UDP socket.
    // let socket = match UdpSocket::bind((sat_ip, port)) {
    //     Ok(sock) => sock,
    //     Err(e) => return log_error(&data, e.to_string()).unwrap(),
    // };

    debug!("Starting downlink endpoint {:?}", &port);

    let (packet_tx, packet_rx) = mpsc::channel();
    let (return_tx, return_rx) = mpsc::channel();
    let num_packets = Arc::new(AtomicU32::new(0));

    let max = 32;

    let data_c = data.clone();
    let num_packets_c = num_packets.clone();

    // This thread receives data for downlink, buffers it and puts it in a fifo.
    // The number of buffers is limited, the thread will loop/wait for buffers to be released then
    // continue.
    let port_c = port.clone();
    thread::Builder::new()
        .stack_size(4 * 1024)
        .spawn(move || {
            let buf_size = port_c.buf_size.unwrap_or(8 * 1024);
            let port = port_c.port;
            info!(
                "Starting UDP receiving thread for {}, buf_size: {}",
                &port, &buf_size
            );
            let data = data_c;
            let num_packets = num_packets_c;
            // Bind the downlink endpoint to a UDP socket.
            let socket = match UdpSocket::bind((sat_ip, port)) {
                Ok(sock) => sock,
                Err(e) => return log_error(&data, e.to_string()).unwrap(),
            };

            let mut buf: Option<Vec<u8>> = None;
            loop {
                if let None = &buf {
                    buf = Some(match return_rx.try_recv() {
                        Ok(buf) => buf,
                        Err(_) => {
                            let num_pkts = num_packets.load(Ordering::SeqCst);
                            if num_pkts >= max {
                                std::thread::yield_now();
                                continue;
                            } else {
                                debug!("Created new buffer for {}", &port);
                                vec![0; buf_size]
                            }
                        }
                    });
                }

                if let Some(mut mut_buf) = buf.take() {
                    // Indefinitely wait for a message from any application or service.
                    let (size, address) = match socket.recv_from(&mut mut_buf) {
                        Ok(tuple) => tuple,
                        Err(e) => {
                            log_error(&data, e.to_string()).unwrap();
                            buf = Some(mut_buf);
                            continue;
                        }
                    };

                    if let Err(SendError((_size, _address, bad_buf))) =
                        packet_tx.send((size, address, mut_buf))
                    {
                        error!("Failed to send packet to channel");
                        buf = Some(bad_buf);
                        continue;
                    }

                    num_packets.fetch_add(1, Ordering::SeqCst);
                }
            }
        })
        .unwrap();

    // This socket is used specifically for sending backpreassure to the client
    let socket = UdpSocket::bind("0.0.0.0:0").unwrap();

    // Take the packets from the FIFO and downlink them.
    // Also tell the sender how many packets we want from them.
    while let Ok((size, address, buf)) = packet_rx.recv() {
        if let Some(num_pkts) = num_packets
            .fetch_update(
                |x| match x {
                    x if x > 0 => Some(x - 1),
                    _ => None,
                },
                Ordering::SeqCst,
                Ordering::SeqCst,
            )
            .ok()
        {
            // tell the sender how many packets they're allowed to send us.
            let msg = &[max as u8 - std::cmp::min(num_pkts, max) as u8];
            if let Err(e) = socket.send_to(msg, address) {
                debug!("Could not send backpreassure: {:?}", e);
            }
        }

        // Take received message and wrap it in a Link packet.
        // Setting port to 0 because we don't know the ground port...
        // That is known by the ground comms service
        let packet = match Packet::build(0, PayloadType::UDP, port.port, &buf[0..size])
            .and_then(|packet| packet.to_bytes())
        {
            Ok(packet) => packet,
            Err(e) => {
                log_error(&data, e.to_string()).unwrap();
                continue;
            }
        };

        // Write packet to the gateway and update telemetry.
        match write(&write_conn.clone(), &packet) {
            Ok(_) => {
                log_telemetry(&data, &TelemType::Down).unwrap();
                // info!("Packet successfully downlinked");
            }
            Err(e) => {
                log_telemetry(&data, &TelemType::DownFailed).unwrap();
                log_error(&data, e.to_string()).unwrap();
                error!("Packet failed to downlink");
            }
        };

        if let Err(_) = return_tx.send(buf) {
            error!("Dropping packet as failed to send back to udp thread");
        }
    }
}
