import { useSimulation } from './hooks/useSimulation.js';
import { Header } from './components/Header.jsx';
import { Controls } from './components/Controls.jsx';
import { BufferView, SeatMap, WaitlistView } from './components/StateViews.jsx';
import { ThreadMonitor, LockMonitor, EventLog, StatsView } from './components/Monitors.jsx';
import { RaceResult, EduPanel, RequestTable } from './components/Extras.jsx';
import { TestResults } from './components/TestResults.jsx';

export default function App() {
  const { snap, connected, cmd } = useSimulation();
  return (
    <div className="min-h-screen p-3 sm:p-5 max-w-7xl mx-auto space-y-3">
      <Header snap={snap} connected={connected} />
      <div className="grid gap-3 lg:grid-cols-3">
        <Controls snap={snap} cmd={cmd} />
        <BufferView snap={snap} />
        <LockMonitor snap={snap} />
      </div>
      <div className="grid gap-3 lg:grid-cols-3">
        <SeatMap snap={snap} />
        <WaitlistView snap={snap} />
        <ThreadMonitor snap={snap} />
      </div>
      <RaceResult snap={snap} />
      <TestResults />
      <div className="grid gap-3 lg:grid-cols-2">
        <EventLog snap={snap} />
        <div className="space-y-3">
          <StatsView snap={snap} />
          <RequestTable snap={snap} />
        </div>
      </div>
      <EduPanel />
      <footer className="text-center text-[11px] text-slate-500 pb-4">
        OS practical — Producer–Consumer · bounded buffer · mutex · condition variables · race prevention · FIFO waiting list
      </footer>
    </div>
  );
}
