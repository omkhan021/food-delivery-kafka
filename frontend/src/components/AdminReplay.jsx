import { useEffect, useState } from 'react';
import {
  getKafkaTopics,
  getConsumerGroups,
  replayFromBeginning,
  getOrderEventLog,
} from '../api';
import { CONSUMER_GROUPS } from '../config';

function formatTime(ts) {
  if (!ts) return '';
  try {
    return new Date(ts).toLocaleString();
  } catch {
    return ts;
  }
}

export default function AdminReplay() {
  const [topics, setTopics] = useState([]);
  const [topicsError, setTopicsError] = useState(null);
  const [topicsLoading, setTopicsLoading] = useState(false);

  const [groups, setGroups] = useState([]);
  const [groupsError, setGroupsError] = useState(null);
  const [groupsLoading, setGroupsLoading] = useState(false);

  const [selectedGroup, setSelectedGroup] = useState(CONSUMER_GROUPS[0]);
  const [replayResult, setReplayResult] = useState(null);
  const [replayError, setReplayError] = useState(null);
  const [replaying, setReplaying] = useState(false);

  const [eventLogOrderId, setEventLogOrderId] = useState('');
  const [eventLog, setEventLog] = useState(null);
  const [eventLogError, setEventLogError] = useState(null);
  const [eventLogLoading, setEventLogLoading] = useState(false);

  async function loadTopics() {
    setTopicsLoading(true);
    setTopicsError(null);
    try {
      const data = await getKafkaTopics();
      setTopics(Array.isArray(data) ? data : []);
    } catch (err) {
      setTopicsError(err.message || 'Failed to load topics.');
    } finally {
      setTopicsLoading(false);
    }
  }

  async function loadGroups() {
    setGroupsLoading(true);
    setGroupsError(null);
    try {
      const data = await getConsumerGroups();
      setGroups(Array.isArray(data) ? data : []);
    } catch (err) {
      setGroupsError(err.message || 'Failed to load consumer groups.');
    } finally {
      setGroupsLoading(false);
    }
  }

  useEffect(() => {
    loadTopics();
    loadGroups();
  }, []);

  async function handleReplay() {
    setReplaying(true);
    setReplayError(null);
    setReplayResult(null);
    try {
      const result = await replayFromBeginning(selectedGroup);
      setReplayResult(result);
    } catch (err) {
      setReplayError(err.message || 'Replay failed.');
    } finally {
      setReplaying(false);
    }
  }

  async function handleEventLogLookup() {
    const id = eventLogOrderId.trim();
    if (!id) return;
    setEventLogLoading(true);
    setEventLogError(null);
    setEventLog(null);
    try {
      const data = await getOrderEventLog(id);
      setEventLog(Array.isArray(data) ? data : []);
    } catch (err) {
      setEventLogError(err.message || 'Failed to load event log.');
    } finally {
      setEventLogLoading(false);
    }
  }

  return (
    <div className="panel">
      <div className="panel-header">
        <h2>Admin / Replay</h2>
        <p className="panel-subtitle">
          Inspect Kafka topics and consumer-group lag, trigger a replay from the beginning of a
          topic, and prove reprocessing via the event log&apos;s <code>timesSeen</code> counter.
        </p>
      </div>

      <div className="card">
        <div className="table-toolbar">
          <h3 className="card-title">Kafka Topics</h3>
          <button className="btn btn-secondary" onClick={loadTopics} disabled={topicsLoading}>
            {topicsLoading ? 'Loading…' : '↻ Refresh'}
          </button>
        </div>
        {topicsError && <div className="alert alert-error">{topicsError}</div>}
        <div className="table-wrap">
          <table className="data-table">
            <thead>
              <tr>
                <th>Topic</th>
                <th>Partitions</th>
                <th>End Offsets</th>
              </tr>
            </thead>
            <tbody>
              {topics.length === 0 && !topicsLoading && (
                <tr>
                  <td colSpan={3} className="empty-state">
                    No topic data.
                  </td>
                </tr>
              )}
              {topics.map((t, idx) => (
                <tr key={t.topic || idx}>
                  <td className="mono">{t.topic}</td>
                  <td>{t.partitionCount ?? '—'}</td>
                  <td className="mono">
                    {Array.isArray(t.partitions)
                      ? t.partitions.map((p) => `p${p.partition}:${p.endOffset}`).join(', ')
                      : '—'}
                  </td>
                </tr>
              ))}
            </tbody>
          </table>
        </div>
      </div>

      <div className="card">
        <div className="table-toolbar">
          <h3 className="card-title">Consumer Groups &amp; Lag</h3>
          <button className="btn btn-secondary" onClick={loadGroups} disabled={groupsLoading}>
            {groupsLoading ? 'Loading…' : '↻ Refresh'}
          </button>
        </div>
        {groupsError && <div className="alert alert-error">{groupsError}</div>}
        <div className="table-wrap">
          <table className="data-table">
            <thead>
              <tr>
                <th>Group ID</th>
                <th>State</th>
                <th>Per-Partition Lag</th>
              </tr>
            </thead>
            <tbody>
              {groups.length === 0 && !groupsLoading && (
                <tr>
                  <td colSpan={3} className="empty-state">
                    No consumer-group data.
                  </td>
                </tr>
              )}
              {groups.map((g, idx) => (
                <tr key={g.groupId || idx}>
                  <td className="mono">{g.groupId}</td>
                  <td>{g.state}</td>
                  <td className="mono">
                    {Array.isArray(g.partitionLags)
                      ? `total ${g.totalLag ?? '—'} — ` +
                        g.partitionLags
                          .map((p) => `${p.topic}-p${p.partition}: ${p.lag}`)
                          .join(', ')
                      : '—'}
                  </td>
                </tr>
              ))}
            </tbody>
          </table>
        </div>
      </div>

      <div className="card">
        <h3 className="card-title">Replay From Beginning</h3>
        <div className="replay-controls">
          <select
            className="text-input"
            value={selectedGroup}
            onChange={(e) => setSelectedGroup(e.target.value)}
          >
            {CONSUMER_GROUPS.map((g) => (
              <option key={g} value={g}>
                {g}
              </option>
            ))}
          </select>
          <button className="btn btn-primary" onClick={handleReplay} disabled={replaying}>
            {replaying ? 'Replaying…' : 'Replay from beginning'}
          </button>
        </div>
        <p className="hint-text">
          Note: this resets the selected consumer group&apos;s committed offsets to the earliest
          available offset on its subscribed topic(s). If that service&apos;s listener is actively
          polling, it may pick the reset up immediately — but for a clean, guaranteed replay you
          may need to also restart that backend service process afterward so its listener
          reconnects and re-consumes from the reset offset.
        </p>
        {replayError && <div className="alert alert-error">{replayError}</div>}
        {replayResult && (
          <div className="alert alert-success">
            <pre className="replay-result-pre">{JSON.stringify(replayResult, null, 2)}</pre>
          </div>
        )}
      </div>

      <div className="card">
        <h3 className="card-title">Event Log Lookup</h3>
        <div className="replay-controls">
          <input
            type="text"
            className="text-input"
            placeholder="ORD-xxxxxxxx"
            value={eventLogOrderId}
            onChange={(e) => setEventLogOrderId(e.target.value)}
            onKeyDown={(e) => e.key === 'Enter' && handleEventLogLookup()}
          />
          <button
            className="btn btn-primary"
            onClick={handleEventLogLookup}
            disabled={eventLogLoading}
          >
            {eventLogLoading ? 'Looking up…' : 'Lookup'}
          </button>
        </div>
        {eventLogError && <div className="alert alert-error">{eventLogError}</div>}
        {eventLog && (
          <div className="table-wrap">
            <table className="data-table">
              <thead>
                <tr>
                  <th>Topic</th>
                  <th>Partition</th>
                  <th>Offset</th>
                  <th>Event Type</th>
                  <th>Times Seen</th>
                  <th>First Seen</th>
                  <th>Last Seen</th>
                </tr>
              </thead>
              <tbody>
                {eventLog.length === 0 && (
                  <tr>
                    <td colSpan={7} className="empty-state">
                      No event-log rows for that order id.
                    </td>
                  </tr>
                )}
                {eventLog.map((row, idx) => (
                  <tr key={idx}>
                    <td className="mono">{row.topic}</td>
                    <td>{row.partition}</td>
                    <td className="mono">{row.kafkaOffset ?? row.offset}</td>
                    <td>{row.eventType}</td>
                    <td>
                      <span
                        className={`times-seen-badge ${
                          (row.timesSeen ?? row.times_seen ?? 1) > 1 ? 'elevated' : ''
                        }`}
                      >
                        {row.timesSeen ?? row.times_seen ?? 1}
                      </span>
                    </td>
                    <td>{formatTime(row.firstSeenAt ?? row.first_seen_at)}</td>
                    <td>{formatTime(row.lastSeenAt ?? row.last_seen_at)}</td>
                  </tr>
                ))}
              </tbody>
            </table>
          </div>
        )}
      </div>
    </div>
  );
}
