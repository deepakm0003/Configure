import { useAppStore } from '../stores/appStore'

export default function SidePanel() {
  const { activeTab, setActiveTab, tickets } = useAppStore()

  const tabs = ['driver', 'tickets', 'accuracy', 'pipeline', 'chaos'] as const

  return (
    <div className="w-96 bg-control-panel border-l border-control-border flex flex-col">
      <div className="flex border-b border-control-border">
        {tabs.map((tab) => (
          <button
            key={tab}
            onClick={() => setActiveTab(tab)}
            className={`flex-1 px-4 py-3 text-sm font-medium capitalize ${
              activeTab === tab
                ? 'bg-control-bg text-white border-b-2 border-blue-500'
                : 'text-gray-400 hover:text-white'
            }`}
          >
            {tab}
          </button>
        ))}
      </div>

      <div className="flex-1 overflow-y-auto p-4">
        {activeTab === 'tickets' && (
          <div className="space-y-2">
            <h3 className="text-lg font-bold mb-4">City Tickets</h3>
            {Object.values(tickets).length === 0 ? (
              <p className="text-gray-500">No tickets yet</p>
            ) : (
              Object.values(tickets).map((ticket) => (
                <div
                  key={ticket.hazard_id}
                  className="bg-control-bg p-3 rounded border border-control-border"
                >
                  <div className="flex justify-between items-start mb-2">
                    <span className="font-medium">{ticket.issue}</span>
                    <span
                      className={`text-xs px-2 py-1 rounded ${
                        ticket.status === 'OPEN'
                          ? 'bg-red-900 text-red-200'
                          : ticket.status === 'REPAIR_DISPATCHED'
                          ? 'bg-yellow-900 text-yellow-200'
                          : 'bg-green-900 text-green-200'
                      }`}
                    >
                      {ticket.status}
                    </span>
                  </div>
                  <p className="text-sm text-gray-400">{ticket.severity_label}</p>
                </div>
              ))
            )}
          </div>
        )}

        {activeTab === 'accuracy' && (
          <div className="text-center text-gray-500 py-8">
            <p>Accuracy charts (Recharts)</p>
            <p className="text-sm mt-2">(Full implementation in repo)</p>
          </div>
        )}

        {activeTab === 'pipeline' && (
          <div className="text-center text-gray-500 py-8">
            <p>Pipeline metrics</p>
            <p className="text-sm mt-2">(Full implementation in repo)</p>
          </div>
        )}

        {activeTab === 'chaos' && (
          <div className="space-y-4">
            <h3 className="text-lg font-bold mb-4">Chaos Controls</h3>
            <button className="w-full bg-blue-600 hover:bg-blue-700 text-white py-2 rounded">
              Add Pothole (Click Map)
            </button>
            <button className="w-full bg-blue-600 hover:bg-blue-700 text-white py-2 rounded">
              Toggle Rain
            </button>
            <button className="w-full bg-orange-600 hover:bg-orange-700 text-white py-2 rounded">
              Add Faulty Vehicle
            </button>
          </div>
        )}
      </div>
    </div>
  )
}
