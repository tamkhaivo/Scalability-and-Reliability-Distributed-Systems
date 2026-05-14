/*Brendan Nichols - CSC 258 Project Dashboard
This is the Dashboard of our project which will eventually be able to show all kinds of analytics for the system.
It now polls DynamoDB for compiled analytics from the distributed backend.
*/
import { useState, useEffect, useRef, useCallback } from "react";
import { createDynamoClient, getRecentAggregations } from "../lib/dynamodb.js";
import "../Dashboard.css";  

const POLL_INTERVAL_MS = 1000;
const WINDOW_SECONDS = 60;

const Dashboard = () => {
  const [metrics, setMetrics] = useState({ sessions: 0, clicks: 0, mouse_moves: 0, p99: 0, p999: 0 });
  const [systemHealth, setSystemHealth] = useState({ ingestionRate: 0, streamLag: 0, activeShards: 0 });
  const [throughputHistory, setThroughputHistory] = useState([]);
  const [latencyHistory, setLatencyHistory] = useState([]);
  const [connectionStatus, setConnectionStatus] = useState("connecting"); // "connected" | "connecting" | "error"
  const [errorDetails, setErrorDetails] = useState(null);
  const [lastUpdated, setLastUpdated] = useState(null);
  const [pollCount, setPollCount] = useState(0);
  const [config, setConfig] = useState(null);
  
  const clientRef = useRef(null);

  const processAggregations = useCallback((aggregations) => {
    let totalClicks = 0;
    let totalMoves = 0;
    let totalEventsInLastSecond = 0;
    let activeSessions = 0;
    let activeShards = 0;
    let maxLag = 0;
    let p99 = 0;
    let p999 = 0;
    
    // Calculate sliding window throughput
    const now = Date.now();
    const oneSecondAgo = now - 1000;

    aggregations.forEach(agg => {
      if (agg.type === "mouse_click" || agg.type === "click") {
        totalClicks += agg.count;
      }
      if (agg.type === "mouse_move") {
        totalMoves += agg.count;
      }
      if (agg.type === "active_sessions" && agg.timestamp >= oneSecondAgo) {
        activeSessions = agg.count;
      }
      if (agg.type === "active_shards" && agg.timestamp >= oneSecondAgo) {
        activeShards = agg.count;
      }
      if (agg.type === "stream_lag" && agg.timestamp >= oneSecondAgo) {
        maxLag = agg.count;
      }
      if (agg.type === "latency_p99" && agg.timestamp >= oneSecondAgo) {
        p99 = agg.count;
      }
      if (agg.type === "latency_p999" && agg.timestamp >= oneSecondAgo) {
        p999 = agg.count;
      }
      
      if (!agg.type.startsWith("latency_") && agg.type !== "active_sessions" && agg.type !== "active_shards" && agg.type !== "stream_lag" && agg.timestamp >= oneSecondAgo) {
        totalEventsInLastSecond += agg.count;
      }
    });

    // Update the metrics values based on the sliding window
    setMetrics(prev => ({
      ...prev,
      sessions: activeSessions > 0 ? activeSessions : prev.sessions, // keep last known if 0
      clicks: totalClicks,
      mouse_moves: totalMoves,
      p99: p99 > 0 ? p99 : prev.p99,
      p999: p999 > 0 ? p999 : prev.p999
    }));

    // Update the system health values
    setSystemHealth(prev => ({
      ...prev,
      ingestionRate: totalEventsInLastSecond,
      streamLag: maxLag,
      activeShards: activeShards > 0 ? activeShards : prev.activeShards
    }));
    
    setThroughputHistory(prev => {
      const newHistory = [...prev, { time: new Date().toLocaleTimeString(), events: totalEventsInLastSecond }];
      return newHistory.slice(-30); // Keep last 30 data points
    });

    setLatencyHistory(prev => {
      const newHistory = [...prev, { time: new Date().toLocaleTimeString(), p99, p999 }];
      return newHistory.slice(-30); // Keep last 30 data points
    });
  }, []);

  useEffect(() => {
    let isMounted = true;
    let pollTimeoutId = null;

    const initAndPoll = async () => {
      try {
        // Try to load dynamic config from the same origin
        const response = await fetch('config.json');
        if (response.ok) {
          const remoteConfig = await response.json();
          console.log("[Dashboard] Loaded remote config:", remoteConfig);
          setConfig(remoteConfig);
          clientRef.current = createDynamoClient(remoteConfig.identityPoolId, remoteConfig.region);
          setConnectionStatus("connecting");
        } else {
          console.warn("[Dashboard] config.json not found on server.");
        }
      } catch (err) {
        console.warn("[Dashboard] Could not load config.json, will retry or fail:", err);
      }
      
      poll();
    };

    const poll = async () => {
      if (!isMounted) return;

      if (!clientRef.current) {
        setConnectionStatus("connecting");
        pollTimeoutId = setTimeout(initAndPoll, 2000);
        return;
      }

      try {
        const aggregations = await getRecentAggregations({
          client: clientRef.current,
          secondsAgo: WINDOW_SECONDS
        });

        if (!isMounted) return;

        setConnectionStatus("connected");
        setErrorDetails(null);
        setLastUpdated(new Date());
        setPollCount(prev => prev + 1);

        if (aggregations.length > 0) {
          processAggregations(aggregations);
        } else {
          setSystemHealth(prev => ({ ...prev, ingestionRate: 0 }));
        }
      } catch (err) {
        if (!isMounted) return;
        
        console.error("Dashboard poll error:", err);
        setConnectionStatus("error");
        setErrorDetails(err.message || "Unknown DynamoDB error");
      } finally {
        if (isMounted) {
          pollTimeoutId = setTimeout(poll, POLL_INTERVAL_MS);
        }
      }
    };

    initAndPoll();

    return () => {
      isMounted = false;
      if (pollTimeoutId) {
        clearTimeout(pollTimeoutId);
      }
    };
  }, [processAggregations]);

  const statusColor = connectionStatus === "connected" ? "#22c55e"
    : connectionStatus === "error" ? "#ef4444"
    : "#f59e0b";

  const statusLabel = connectionStatus === "connected" ? "Live"
    : connectionStatus === "error" ? "Error — Retrying"
    : "Connecting…";

  return (
    <>
      <div className="dash-hero">
        <h1 className="dash-welcome">Analysis Dashboard</h1>
        <div className="connection-status" style={{ marginTop: 8, display: 'flex', alignItems: 'center', justifyContent: 'center', gap: 8, fontSize: 13 }}>
          <span style={{
            width: 8, height: 8, borderRadius: '50%',
            backgroundColor: statusColor,
            display: 'inline-block',
            animation: connectionStatus === 'connected' ? 'pulse 2s infinite' : 'none'
          }} />
          <span style={{ color: '#666', fontWeight: 500 }}>{statusLabel}</span>
          {connectionStatus === "error" && errorDetails && (
            <span style={{ color: '#ef4444', fontSize: 11, maxWidth: 300, overflow: 'hidden', textOverflow: 'ellipsis', whiteSpace: 'nowrap' }}>
              ({errorDetails})
            </span>
          )}
          {lastUpdated && (
            <span style={{ color: '#999', fontSize: 11, marginLeft: 8 }}>
              Last update: {lastUpdated.toLocaleTimeString()} · Polls: {pollCount}
            </span>
          )}
        </div>
      </div>

      <div className="Dashboard-wrapper">
        <main className="Dashboard-page">
          <div className="Session-wrapper">
            <div className="card stat-card"> 
              <div className="card-title">Est. Sessions (Last 60s)</div>
              <div className="stat-value">{metrics.sessions}</div>
            </div>

            <div className="card stat-card">
              <div className="card-title">Clicks (Last 60s)</div>
              <div className="stat-value">{metrics.clicks}</div>
            </div>

            <div className="card stat-card">
              <div className="card-title">Mouse Moves (Last 60s)</div>
              <div className="stat-value">{metrics.mouse_moves}</div>
            </div>

            <div className="card stat-card">
              <div className="card-title">E2E p99 Latency</div>
              <div className="stat-value">{metrics.p99} ms</div>
            </div>

            <div className="card stat-card">
              <div className="card-title">E2E p99.9 Latency</div>
              <div className="stat-value">{metrics.p999} ms</div>
            </div>
          </div>
          
          <div className="Stats-wrapper">
            <div className="card system-health">
              <div className="card-header">
                <div className="card-title">System Health</div>
              </div>
              <div className="card-body">
                <div className="status-row">
                  <span className="status-label">Ingestion Rate</span>
                  <span className="status-value">{systemHealth.ingestionRate} events/sec</span>
                </div>
                <div className="status-row">
                  <span className="status-label">Stream Lag</span>
                  <span className="status-value">{systemHealth.streamLag} ms</span>
                </div>
                <div className="status-row">
                  <span className="status-label">Active Workers</span>
                  <span className="status-value">{systemHealth.activeShards}</span>
                </div>
              </div>
            </div>

            <div className="card system-health">
              <div className="card-header">
                <div className="card-title">Security & Permissions</div>
              </div>
              <div className="card-body">
                <div className="status-row">
                  <span className="status-label">Identity Pool ID</span>
                  <span className="status-value" style={{fontSize: '10px'}}>{config?.identityPoolId || "Loading..."}</span>
                </div>
                <div className="status-row">
                  <span className="status-label">IAM Role</span>
                  <span className="status-value">MessageOrderingPublicPutRole</span>
                </div>
                <div className="status-row">
                  <span className="status-label">Access Level</span>
                  <span className="status-value" style={{color: connectionStatus === 'connected' ? '#22c55e' : '#f59e0b'}}>
                    {connectionStatus === 'connected' ? 'Verified (DynamoDB Read)' : 'Verifying...'}
                  </span>
                </div>
              </div>
            </div>

            <div className="card event-log" style={{gridColumn: 'span 2'}}>
              <div className="card-header">
                <div className="card-title">Latency Trend (ms)</div>
              </div>
              <div className="card-body">
                {latencyHistory.length === 0 ? (
                  <div className="placeholder">Waiting for latency data...</div>
                ) : (
                  <div className="event-list" style={{display: 'grid', gridTemplateColumns: 'repeat(auto-fill, minmax(200px, 1fr))', gap: '8px'}}>
                    {latencyHistory.map((pt, i) => (
                      <div key={i} className="event-item" style={{display: 'flex', flexDirection: 'column', padding: '6px 8px', backgroundColor: '#f8fafc', borderRadius: '4px'}}>
                        <span style={{fontSize: '10px', color: '#64748b', marginBottom: '4px'}}>{pt.time}</span>
                        <div style={{display: 'flex', justifyContent: 'space-between', alignItems: 'center'}}>
                          <span style={{fontSize: '11px', fontWeight: 500}}>p99: <strong>{pt.p99}ms</strong></span>
                          <span style={{fontSize: '11px', fontWeight: 500}}>p99.9: <strong>{pt.p999}ms</strong></span>
                        </div>
                      </div>
                    ))}
                  </div>
                )}
              </div>
            </div>

            <div className="card event-log" style={{gridColumn: 'span 2'}}>
              <div className="card-header">
                <div className="card-title">Traffic (Events/sec)</div>
              </div>
              <div className="card-body">
                {throughputHistory.length === 0 ? (
                  <div className="placeholder">Waiting for stream data...</div>
                ) : (
                  <div className="event-list" style={{display: 'grid', gridTemplateColumns: 'repeat(auto-fill, minmax(200px, 1fr))', gap: '8px'}}>
                    {throughputHistory.map((pt, i) => (
                      <div key={i} className="event-item" style={{display: 'flex', justifyContent: 'space-between', padding: '4px 8px', backgroundColor: '#f8fafc', borderRadius: '4px'}}>
                        <span style={{fontSize: '11px', color: '#64748b'}}>{pt.time}</span>
                        <strong style={{fontSize: '12px'}}>{pt.events} ev/s</strong>
                      </div>
                    ))}
                  </div>
                )}
              </div>
            </div>
          </div>
        </main>
      </div>
    </>
  );
};

export default Dashboard;
