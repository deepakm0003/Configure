import { useAppStore } from '../stores/appStore'

const API_WS = import.meta.env.VITE_API_WS || 'ws://localhost:8000/ws'

export function connectWebSocket() {
  let ws: WebSocket | null = null
  let reconnectTimer: NodeJS.Timeout | null = null

  function connect() {
    ws = new WebSocket(API_WS)

    ws.onopen = () => {
      console.log('[WS] Connected')
      if (reconnectTimer) {
        clearTimeout(reconnectTimer)
        reconnectTimer = null
      }
    }

    ws.onmessage = (event) => {
      const data = JSON.parse(event.data)
      const { setSnapshot, updateHazard, updateVehicles, updateScore, updateMetrics, updateReputation } = useAppStore.getState()

      switch (data.type) {
        case 'snapshot':
          setSnapshot(data)
          break
        case 'hazard_update':
          if (data.hazard) updateHazard(data.hazard)
          break
        case 'vehicle_positions':
          if (data.vehicles) updateVehicles(data.vehicles)
          break
        case 'score_update':
          updateScore(data)
          break
        case 'pipeline_stats':
          updateMetrics(data)
          break
        case 'reputation_update':
          if (data.reputation) updateReputation(data.reputation)
          break
        case 'ping':
          // Heartbeat - no action needed
          break
      }
    }

    ws.onerror = (error) => {
      console.error('[WS] Error:', error)
    }

    ws.onclose = () => {
      console.log('[WS] Disconnected, reconnecting in 3s...')
      reconnectTimer = setTimeout(connect, 3000)
    }
  }

  connect()
  return ws
}
