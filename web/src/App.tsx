import { useEffect } from 'react'
import { useAppStore } from './stores/appStore'
import KPIBar from './components/KPIBar'
import MapView from './components/MapView'
import SidePanel from './components/SidePanel'
import { connectWebSocket } from './lib/websocket'

function App() {
  useEffect(() => {
    const ws = connectWebSocket()
    return () => ws?.close()
  }, [])

  return (
    <div className="flex flex-col h-screen bg-control-bg">
      <KPIBar />
      <div className="flex-1 flex overflow-hidden">
        <MapView />
        <SidePanel />
      </div>
    </div>
  )
}

export default App
