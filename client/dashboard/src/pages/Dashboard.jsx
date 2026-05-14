/*Brendan Nichols - CSC 258 Project Dashboard
This is the Dashboard of our project which will eventually be able to show all kinds of analytics for the system.
Everything is currently hard coded, but will accurately show off the data by the end of the project
*/
import React, { useState, useEffect, useRef } from "react";
import { createKinesisClient, listActiveShards, getShardIterator, getKinesisRecords } from "../lib/kinesis.js";
import "../Dashboard.css";  

const Dashboard = () => {
  const [metrics, setMetrics] = useState({ sessions: 0, clicks: 0 });
  const [systemHealth, setSystemHealth] = useState({ ingestionRate: 0, streamLag: 0, activeShards: 0 });
  const [events, setEvents] = useState([]);
  
  const clientRef = useRef(null);
  const iteratorRef = useRef(null);
  const sessionSet = useRef(new Set());

  useEffect(() => {
    const startDataConsumption = async () => {
      try {
        const client = createKinesisClient();
        clientRef.current = client;

        const shards = await listActiveShards(client);
        setHealth(prev => ({ ...prev, activeShards: shards.length }));

        if (shards.length > 0) {
          const shardId = shards.ShardId;
          iteratorRef.current = await getShardIterator({ client, shardId });
        }

        const pollId = setInterval(async () => {
          if (!iteratorRef.current) return;

          const { records, nextIterator, millisBehindLatest } = await getKinesisRecords({
            client: clientRef.current,
            shardIterator: iteratorRef.current
          });

          if (records.length > 0) {
            processIncomingRecords(records);
          } else {
             // Resets ingestion rate if no new records during the poll
             setHealth(prev => ({ ...prev, ingestionRate: 0 }));
          }

          iteratorRef.current = nextIterator;
        }, 1000);

        return () => clearInterval(pollId);
      } catch (err) {
        console.error("Dashboard Connection Error:", err);
      }
    };

    startDataConsumption();
  }, []);

  const processRecords = (newRecords) => {
    let newClicks = 0;
    const now = Date.now();

    newRecords.forEach(record => {
      if (record.sessionId) sessionSet.current.add(record.sessionId);

      if (record.type === "click" || (record.elementId && record.elementId.startsWith("btn-"))) {
        newClicks++;
      }
    });

    setEvents(prev => [...newRecords, ...prev].slice(0, 30));

    // Update the metrics values
    setMetrics(prev => ({
      ...prev,
      sessions: sessionSet.current.size,
      clicks: prev.clicks + newClicks
    }));

    // Update the system health values
    setHealth(prev => ({
      ...prev,
      ingestionRate: newRecords.length,
      streamLag: millisBehindLatest
    }));
  };

  return (
    <>
      <div className="dash-hero">
        <h1 className="dash-welcome">Analysis Dashboard</h1>
      </div>

      <div className="Dashboard-wrapper">
        <main className="Dashboard-page">
          <div className="Session-wrapper">
            <div className="card stat-card"> 
              <div className="card-title">Sessions</div>
              <div className="stat-value">{metrics.sessions}</div>
            </div>

            <div className="card stat-card">
              <div className="card-title">Clicks</div>
              <div className="stat-value">{metrics.clicks}</div>
            </div>

            <div className="card stat-card">
              <div className="card-title">Avg Session</div>
              <div className="stat-value">—</div>
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
                  <span className="status-label">Active Shards</span>
                  <span className="status-value">{systemHealth.activeShards}</span>
                </div>
              </div>
            </div>

            <div className="card main-visual">
              <div className="card-header">
                <div className="card-title">Mouse Heatmap (Live)</div>
              </div>
              <div className="card-body">
                <div className="placeholder">
                  Real-time mouse tracking will render here
                </div>
              </div>
            </div>

            <div className="card event-log">
              <div className="card-header">
                <div className="card-title">Traffic (Events/sec)</div>
              </div>
              <div className="card-body">
                <div className="placeholder">
                  Throughput graph will render here
                </div>
              </div>
            </div>

            <div className="card event-log">
              <div className="card-header">
                <div className="card-title">Live Event Stream</div>
              </div>
              <div className="card-body">
                {events.length === 0 ? (
                  <div className="placeholder">Waiting for stream data...</div>
                ) : (
                  <div className="event-list">
                    {events.map((ev, i) => (
                      <div key={i} className="event-item">
                        <strong>{ev.type}</strong> - {ev.elementId || "Mouse Movement"}
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