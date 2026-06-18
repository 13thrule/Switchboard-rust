#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ConnectionState {
    Handshake,
    Ready,
    Closed,
}

impl ConnectionState {
    #[must_use]
    pub fn on_first_frame(self) -> Self {
        match self {
            ConnectionState::Handshake => ConnectionState::Ready,
            other => other,
        }
    }

    #[must_use]
    pub fn close(self) -> Self {
        ConnectionState::Closed
    }

    pub fn is_closed(&self) -> bool {
        matches!(self, ConnectionState::Closed)
    }

    pub fn is_ready(&self) -> bool {
        matches!(self, ConnectionState::Ready)
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum MessageState {
    Parsed,
    Routed,
    Delivered,
}

impl MessageState {
    #[must_use]
    pub fn on_routed(self) -> Self {
        match self {
            MessageState::Parsed => MessageState::Routed,
            other => other,
        }
    }

    #[must_use]
    pub fn on_delivered(self) -> Self {
        match self {
            MessageState::Routed => MessageState::Delivered,
            other => other,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn connection_state_transitions() {
        let s = ConnectionState::Handshake;
        let s = s.on_first_frame();
        assert_eq!(s, ConnectionState::Ready);

        let s = s.close();
        assert_eq!(s, ConnectionState::Closed);

        let s = s.on_first_frame();
        assert_eq!(s, ConnectionState::Closed);
    }

    #[test]
    fn message_state_transitions() {
        let s = MessageState::Parsed;
        let s = s.on_routed();
        assert_eq!(s, MessageState::Routed);

        let s = s.on_delivered();
        assert_eq!(s, MessageState::Delivered);
    }
}
