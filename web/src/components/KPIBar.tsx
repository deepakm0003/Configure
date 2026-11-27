import { useAppStore } from '../stores/appStore'

export default function KPIBar() {
  const { vehicles, hazards, tickets, score, metrics } = useAppStore()

  const vehiclesOnline = Object.keys(vehicles).length
  const confirmedHazards = Object.values(hazards).filter(h => h.status === 'CONFIRMED').length
  const openTickets = Object.values(tickets).filter(t => t.status === 'OPEN').length
  const msgsPerS = metrics?.msgs_per_s?.alert_in || 0
  const latencyP95 = metrics?.latency_ms?.emit_to_alert_p95 || 0

  return (
    <div className="bg-control-panel border-b border-control-border px-6 py-3 flex gap-8 items-center text-sm">
      <div className="flex items-center gap-2">
        <div className="w-2 h-2 bg-green-500 rounded-full animate-pulse"></div>
        <span className="text-gray-400">Vehicles:</span>
        <span className="font-bold text-white">{vehiclesOnline}</span>
      </div>
      
      <div className="flex items-center gap-2">
        <span className="text-gray-400">Msgs/s:</span>
        <span className="font-bold text-white">{msgsPerS.toFixed(0)}</span>
      </div>
      
      <div className="flex items-center gap-2">
        <span className="text-gray-400">Latency P95:</span>
        <span className="font-bold text-white">{latencyP95.toFixed(0)}ms</span>
      </div>
      
      <div className="flex items-center gap-2">
        <span className="text-orange-400">●</span>
        <span className="text-gray-400">Confirmed:</span>
        <span className="font-bold text-white">{confirmedHazards}</span>
      </div>
      
      <div className="flex items-center gap-2">
        <span className="text-red-400">●</span>
        <span className="text-gray-400">Open Tickets:</span>
        <span className="font-bold text-white">{openTickets}</span>
      </div>
      
      {score && (
        <>
          <div className="flex items-center gap-2 ml-auto">
            <span className="text-gray-400">Precision:</span>
            <span className="font-bold text-green-400">{(score.precision * 100).toFixed(1)}%</span>
          </div>
          <div className="flex items-center gap-2">
            <span className="text-gray-400">Recall:</span>
            <span className="font-bold text-blue-400">{(score.recall * 100).toFixed(1)}%</span>
          </div>
        </>
      )}
    </div>
  )
}
