//! Which real-time options each worker thread and socket accepts, and
//! applying them.

use std::io;
use std::net::{SocketAddr, UdpSocket};

use fast_talker::options::{Report, ReportSummary, Rules, SocketOption, ThreadOption};
use fast_talker::rt::{Scheduler, ThreadPriority};
use fast_talker::sockets::{self, OpenError};

use crate::error::CameraError;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum ThreadRole {
    /// The GVCP worker: request/response, heartbeat, events.
    Control,
    /// The GVSP worker: reassembly and fan-out at line rate.
    Stream,
}

impl ThreadRole {
    fn driver(self) -> &'static str {
        match self {
            Self::Control => "gvcp",
            Self::Stream => "gvsp",
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum SocketRole {
    /// The GVCP socket: commands, acknowledges and device events.
    UdpControl,
    /// The GVSP socket: receive-only image data.
    UdpStreamRx,
}

impl SocketRole {
    fn driver(self) -> &'static str {
        match self {
            Self::UdpControl => "gvcp",
            Self::UdpStreamRx => "gvsp",
        }
    }
}

pub(crate) fn thread_allowed(role: ThreadRole, o: &ThreadOption) -> bool {
    match role {
        ThreadRole::Stream => match o {
            ThreadOption::CpuAffinity(_)
            | ThreadOption::RtPriority(_)
            | ThreadOption::PrefaultStack(_)
            | ThreadOption::UnixScheduler(_)
            | ThreadOption::LinuxNice(_)
            | ThreadOption::WinPriority(_)
            | ThreadOption::WinDisablePowerThrottling
            | ThreadOption::WinMmcss(_)
            | ThreadOption::MacOsQos(_) => true,
            ThreadOption::MacOsTimeConstraint { .. } => false,
            _ => false,
        },
        ThreadRole::Control => match o {
            ThreadOption::CpuAffinity(_)
            | ThreadOption::PrefaultStack(_)
            | ThreadOption::LinuxNice(_)
            | ThreadOption::WinDisablePowerThrottling
            | ThreadOption::MacOsQos(_) => true,
            ThreadOption::UnixScheduler(s) => {
                matches!(s, Scheduler::Other | Scheduler::Batch | Scheduler::Idle)
            }
            ThreadOption::WinPriority(p) => *p != ThreadPriority::TimeCritical,
            _ => false,
        },
    }
}

pub(crate) fn socket_allowed(role: SocketRole, o: &SocketOption) -> bool {
    match role {
        SocketRole::UdpStreamRx => matches!(
            o,
            SocketOption::RecvBuffer(_)
                | SocketOption::BindDevice(_)
                | SocketOption::LinuxBusyPoll(_)
                | SocketOption::LinuxPreferBusyPoll(_)
                | SocketOption::LinuxBusyPollBudget(_)
                | SocketOption::WinCpuAffinity(_)
        ),
        SocketRole::UdpControl => matches!(
            o,
            SocketOption::RecvBuffer(_)
                | SocketOption::BindDevice(_)
                | SocketOption::Dscp(_)
                | SocketOption::LinuxPriority(_)
        ),
    }
}

pub(crate) fn check_thread(role: ThreadRole, options: &[ThreadOption]) -> Result<(), CameraError> {
    let allow = |o: &ThreadOption| thread_allowed(role, o);
    match Rules::portable(&allow).first_rejected(options) {
        Some(o) => Err(invalid(o.kind_name(), o, role.driver())),
        None => Ok(()),
    }
}

pub(crate) fn check_socket(role: SocketRole, options: &[SocketOption]) -> Result<(), CameraError> {
    let allow = |o: &SocketOption| socket_allowed(role, o);
    match Rules::portable(&allow).first_rejected(options) {
        Some(o) => Err(invalid(o.kind_name(), o, role.driver())),
        None => Ok(()),
    }
}

fn invalid(kind: &str, option: &impl std::fmt::Debug, driver: &'static str) -> CameraError {
    CameraError::InvalidOption {
        option: format!("{kind} ({option:?})"),
        driver,
    }
}

/// What a worker's thread and socket options came to.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct TuningReport {
    /// The worker thread's options.
    pub thread: ReportSummary<ThreadOption>,
    /// The worker socket's options.
    pub socket: ReportSummary<SocketOption>,
}

/// Applies `options` to the calling thread. Keep the returned report alive
/// for as long as the thread runs: some options are released when it drops.
pub(crate) fn apply_thread(
    role: ThreadRole,
    options: &[ThreadOption],
) -> io::Result<Report<ThreadOption>> {
    let allow = |o: &ThreadOption| thread_allowed(role, o);
    let report = ThreadOption::apply_all(options, &Rules::portable(&allow))?;
    log(role.driver(), "thread", &report);
    Ok(report)
}

/// Creates a UDP socket, applies `options` to it, and binds it to `addr`, so
/// the options that only take effect before bind do.
pub(crate) fn bind_udp(
    role: SocketRole,
    addr: SocketAddr,
    options: &[SocketOption],
) -> Result<(UdpSocket, Report<SocketOption>), OpenError> {
    let allow = |o: &SocketOption| socket_allowed(role, o);
    let (socket, report) = sockets::bind_udp(addr, options, &Rules::portable(&allow))?;
    log(role.driver(), "socket", &report);
    Ok((socket, report))
}

/// Carries the busy polling `options` declare over to the epoll `poll` waits
/// on, so the runner spins for the next packet instead of sleeping until the
/// NIC's interrupt (Linux 6.9+). A kernel without the ioctl is logged and the
/// socket's own busy polling stays; any other refusal fails the connect, as
/// a refused socket option does.
pub(crate) fn apply_epoll_busy_poll(
    role: SocketRole,
    poll: &mio::Poll,
    options: &[SocketOption],
) -> io::Result<()> {
    let Some(p) = sockets::epoll_busy_poll_from(options) else {
        return Ok(());
    };
    let driver = role.driver();
    #[cfg(target_os = "linux")]
    {
        use std::os::fd::AsFd;
        match sockets::set_epoll_busy_poll(poll.registry().as_fd(), &p) {
            Ok(()) => tracing::info!(
                usecs = p.usecs,
                budget = p.budget,
                prefer = p.prefer,
                "{driver} poll wait busy-polls the NIC queue"
            ),
            Err(e) if e.kind() == io::ErrorKind::Unsupported => tracing::warn!(
                error = %e,
                "{driver} poll wait cannot busy-poll; it sleeps until the interrupt"
            ),
            Err(e) => {
                return Err(io::Error::new(
                    e.kind(),
                    format!("epoll busy polling {p:?}: {e}"),
                ));
            }
        }
    }
    #[cfg(not(target_os = "linux"))]
    let _ = (driver, poll, p);
    Ok(())
}

fn log<O: std::fmt::Debug>(driver: &str, what: &str, report: &Report<O>) {
    for a in &report.adjusted {
        tracing::warn!(
            option = ?a.option,
            effective = ?a.effective,
            reason = %a.reason,
            "{driver} {what} option adjusted"
        );
    }
    for s in &report.skipped {
        tracing::warn!(
            option = ?s.option,
            reason = %s.reason,
            "{driver} {what} option skipped"
        );
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn control_thread_refuses_real_time_classes() {
        let refused = [
            ThreadOption::RtPriority(80),
            ThreadOption::UnixScheduler(Scheduler::Fifo(10)),
            ThreadOption::UnixScheduler(Scheduler::RoundRobin(10)),
            ThreadOption::WinPriority(ThreadPriority::TimeCritical),
            ThreadOption::WinMmcss("Pro Audio".into()),
            ThreadOption::MacOsTimeConstraint {
                period_us: 1000,
                computation_us: 100,
                constraint_us: 500,
            },
        ];
        for o in &refused {
            assert!(!thread_allowed(ThreadRole::Control, o), "{o:?}");
        }
        assert!(thread_allowed(
            ThreadRole::Control,
            &ThreadOption::UnixScheduler(Scheduler::Batch)
        ));
        assert!(thread_allowed(
            ThreadRole::Control,
            &ThreadOption::WinPriority(ThreadPriority::Highest)
        ));
    }

    #[test]
    fn stream_thread_refuses_only_time_constraint() {
        assert!(thread_allowed(
            ThreadRole::Stream,
            &ThreadOption::RtPriority(80)
        ));
        assert!(!thread_allowed(
            ThreadRole::Stream,
            &ThreadOption::MacOsTimeConstraint {
                period_us: 1000,
                computation_us: 100,
                constraint_us: 500,
            }
        ));
    }

    #[test]
    fn socket_lists() {
        assert!(socket_allowed(
            SocketRole::UdpStreamRx,
            &SocketOption::RecvBuffer(1 << 20)
        ));
        assert!(!socket_allowed(
            SocketRole::UdpStreamRx,
            &SocketOption::Dscp(46)
        ));
        assert!(socket_allowed(
            SocketRole::UdpStreamRx,
            &SocketOption::WinCpuAffinity(0)
        ));
        assert!(socket_allowed(
            SocketRole::UdpControl,
            &SocketOption::Dscp(46)
        ));
        assert!(!socket_allowed(
            SocketRole::UdpControl,
            &SocketOption::LinuxBusyPoll(50)
        ));
        let err = check_socket(SocketRole::UdpStreamRx, &[SocketOption::SendBuffer(1)]);
        assert!(matches!(
            err,
            Err(CameraError::InvalidOption { driver: "gvsp", option }) if option == "send_buffer (SendBuffer(1))"
        ));
    }
}
