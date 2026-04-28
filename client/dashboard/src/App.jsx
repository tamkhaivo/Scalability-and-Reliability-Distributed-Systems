/*Brendan Nichols - CSC 258 Project Dashboard
This is the Dashboard's App file which I can use as a router to other pages, right now just goes to Dashboard
*/
import { BrowserRouter as Router, Routes, Route } from "react-router-dom";
import Dashboard from "./pages/Dashboard.jsx";

function App() {
  return (
    <Router>
      <Routes>
        <Route path="/" element={<Dashboard />} />
      </Routes>
    </Router>
  );
}

export default App;