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
  const [mousePoints, setMousePoints] = useState([]);
  
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
    const newMousePoints = [];

    newRecords.forEach(record => {
      const { eventType, data } = record;

      if (record.PartitionKey) {
        const sessionId = record.PartitionKey.split('-');
        sessionSet.current.add(sessionId);
      }

      if (eventType === "mouse_click" || data.target?.id?.startsWith("btn-")) {
        newClicks++;
      }

      if (eventType === "mouse_move" || eventType === "mouse_click") {
        newMousePoints.push({
          x: data.position.x,
          y: data.position.y,
          isClick: eventType === "mouse_click",
          id: Math.random()
        });
      }
    });

    //For "live event stream"
    setEvents(prev => [...newRecords, ...prev].slice(0, 30));

    //For "mouse heatmap"
    setMousePoints(prev => [...newMousePoints, ...prev].slice(0, 50));

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
                  {mousePoints.map((point) => (
                  <div
                    key={point.id}
                    style={{
                      position: 'absolute',
                      left: `${point.x}%`,
                      top: `${point.y}%`,
                      width: point.isClick ? '12px' : '6px',
                      height: point.isClick ? '12px' : '6px',
                      borderRadius: '50%',
                      backgroundColor: point.isClick ? '#ff4d4f' : '#1890ff',
                      opacity: 0.6,
                      transform: 'translate(-50%, -50%)'
                    }}
                  />
                ))}
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