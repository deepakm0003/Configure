export default function MapView() {
  return (
    <div className="flex-1 relative bg-gray-900">
      <div className="absolute inset-0 flex items-center justify-center text-gray-500">
        <div className="text-center">
          <div className="text-6xl mb-4">🗺️</div>
          <p className="text-xl">MapLibre + deck.gl Map</p>
          <p className="text-sm mt-2">(Full implementation in repo)</p>
        </div>
      </div>
    </div>
  )
}
