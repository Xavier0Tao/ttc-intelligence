const LABELS = {
  connected: 'Connected',
  reconnecting: 'Reconnecting…',
  disconnected: 'Disconnected',
};

export default function ConnectionStatus({ status }) {
  return (
    <div className={`conn-status conn-${status}`}>
      <span className="conn-dot" />
      {LABELS[status] || status}
    </div>
  );
}
