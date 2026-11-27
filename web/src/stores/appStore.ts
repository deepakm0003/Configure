import { create } from 'zustand'

export interface Vehicle {
  id: string
  c: string  // class
  lat: number
  lon: number
  h: number  // heading
  s: number  // speed
  ts_ms?: number
}

export interface Hazard {
  hazard_id: string
  hazard_type: string
  status: string
  confidence: number
  severity_score: number
  severity_label: string
  advisory_speed_kmh?: number
  lat: number
  lon: number
  h3_cell: string
  distinct_vehicles: number
  radius_m?: number
}

export interface Reputation {
  vehicle_id: string
  reputation: number
  quarantined: boolean
}

export interface Ticket {
  hazard_id: string
  issue: string
  status: string
  severity_label: string
  lat: number
  lon: number
}

export interface Score {
  precision: number
  recall: number
  f1: number
  confirmed_hazards: number
  ttd_mean_s: number
  ttd_p90_s: number
}

export interface Metrics {
  msgs_per_s: Record<string, number>
  latency_ms: Record<string, number>
}

interface AppState {
  // Data
  vehicles: Record<string, Vehicle>
  hazards: Record<string, Hazard>
  reputation: Record<string, Reputation>
  tickets: Record<string, Ticket>
  score: Score | null
  metrics: Metrics | null
  
  // UI state
  selectedHazard: Hazard | null
  selectedVehicle: string | null
  activeTab: 'driver' | 'tickets' | 'accuracy' | 'pipeline' | 'chaos'
  
  // Actions
  setSnapshot: (snapshot: any) => void
  updateHazard: (hazard: Hazard) => void
  updateVehicles: (vehicles: Vehicle[]) => void
  updateReputation: (rep: Reputation) => void
  updateTicket: (ticket: Ticket) => void
  updateScore: (score: Score) => void
  updateMetrics: (metrics: Metrics) => void
  selectHazard: (hazard: Hazard | null) => void
  selectVehicle: (id: string | null) => void
  setActiveTab: (tab: AppState['activeTab']) => void
}

export const useAppStore = create<AppState>((set) => ({
  vehicles: {},
  hazards: {},
  reputation: {},
  tickets: {},
  score: null,
  metrics: null,
  selectedHazard: null,
  selectedVehicle: null,
  activeTab: 'driver',
  
  setSnapshot: (snapshot) => set({
    vehicles: Object.fromEntries((snapshot.vehicles || []).map((v: Vehicle) => [v.id, v])),
    hazards: Object.fromEntries((snapshot.hazards || []).map((h: Hazard) => [h.hazard_id, h])),
    reputation: Object.fromEntries((snapshot.reputation || []).map((r: Reputation) => [r.vehicle_id, r])),
    tickets: Object.fromEntries((snapshot.tickets || []).map((t: Ticket) => [t.hazard_id, t])),
  }),
  
  updateHazard: (hazard) => set((state) => ({
    hazards: { ...state.hazards, [hazard.hazard_id]: hazard }
  })),
  
  updateVehicles: (vehicles) => set((state) => ({
    vehicles: { ...state.vehicles, ...Object.fromEntries(vehicles.map(v => [v.id, v])) }
  })),
  
  updateReputation: (rep) => set((state) => ({
    reputation: { ...state.reputation, [rep.vehicle_id]: rep }
  })),
  
  updateTicket: (ticket) => set((state) => ({
    tickets: { ...state.tickets, [ticket.hazard_id]: ticket }
  })),
  
  updateScore: (score) => set({ score }),
  updateMetrics: (metrics) => set({ metrics }),
  selectHazard: (hazard) => set({ selectedHazard: hazard }),
  selectVehicle: (id) => set({ selectedVehicle: id }),
  setActiveTab: (tab) => set({ activeTab: tab }),
}))
