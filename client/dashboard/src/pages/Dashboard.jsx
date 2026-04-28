/*Brendan Nichols - CSC 258 Project Dashboard
This is the Dashboard of our project which will eventually be able to show all kinds of analytics for the system.
Everything is currently hard coded, but will accurately show off the data by the end of the project
*/
import "../Dashboard.css";

const Dashboard = () => {
  return (
    <>
      <div className="dash-hero">
        <h1 className="dash-welcome">Analysis Dashboard</h1>
      </div>

      <div className="Dashboard-wrapper">
        <main className="Dashboard-page">

          <div className="card stat-card">
            <div className="card-title">Sessions</div>
            <div className="stat-value">—</div>
          </div>

          <div className="card stat-card">
            <div className="card-title">Clicks</div>
            <div className="stat-value">—</div>
          </div>

          <div className="card stat-card">
            <div className="card-title">Avg Session</div>
            <div className="stat-value">—</div>
          </div>

          <div className="card stat-card">
            <div className="card-title">Bounce Rate</div>
            <div className="stat-value">—</div>
          </div>

          <div className="card system-health">
            <div className="card-header">
              <div className="card-title">System Health</div>
            </div>
            <div className="card-body">
              <div className="status-row">
                <span className="status-label">Ingestion Rate</span>
                <span className="status-value">— events/sec</span>
              </div>
              <div className="status-row">
                <span className="status-label">Stream Lag</span>
                <span className="status-value">— ms</span>
              </div>
              <div className="status-row">
                <span className="status-label">Active Shards</span>
                <span className="status-value">—</span>
              </div>
              <div className="status-row">
                <span className="status-label">Duplicate Events</span>
                <span className="status-value">—</span>
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

          <div className="card side-panel">
            <div className="card-header">
              <div className="card-title">Session Details</div>
            </div>
            <div className="card-body">
              <p className="status-value">
                Select a session to replay interactions
              </p>
            </div>
          </div>

          <div className="card traffic-graph">
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
              <div className="placeholder">
                Mouse move / click events stream here
              </div>
            </div>
          </div>

        </main>
      </div>
    </>
  );
};

export default Dashboard;