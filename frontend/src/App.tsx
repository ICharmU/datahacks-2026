import { BrowserRouter, Route, Routes } from "react-router-dom"
import { Layout } from "./components/Layout"
import { SystemStatusBar } from "./components/SystemStatusBar"
import { useAppMode } from "./hooks/useAppMode"
import { HomePage } from "./pages/HomePage"
import { FleetPage } from "./pages/FleetPage"
import { FleetZoneDetailPage } from "./pages/FleetZoneDetailPage"
import { GrowerPage } from "./pages/GrowerPage"
import { GrowerSiteDetailPage } from "./pages/GrowerSiteDetailPage"
import { MapPage } from "./pages/MapPage"
import { SciencePage } from "./pages/SciencePage"
import { PipelinePage } from "./pages/PipelinePage"
import "./App.css"

export default function App() {
  const { dataMode, setDataMode, shell, setShell, apiModeLabel } = useAppMode()

  return (
    <BrowserRouter>
      <Layout shell={shell} onShellChange={setShell} dataMode={dataMode} onDataModeChange={setDataMode}>
        <SystemStatusBar modeLabel={apiModeLabel} />
        <Routes>
          <Route path="/" element={<HomePage shell={shell} />} />
          <Route path="/fleet" element={<FleetPage dataMode={dataMode} />} />
          <Route path="/fleet/zone/:zoneId" element={<FleetZoneDetailPage dataMode={dataMode} />} />
          <Route path="/grower" element={<GrowerPage dataMode={dataMode} />} />
          <Route path="/grower/site/:siteId" element={<GrowerSiteDetailPage dataMode={dataMode} />} />
          <Route path="/map" element={<MapPage dataMode={dataMode} shell={shell} />} />
          <Route path="/science" element={<SciencePage dataMode={dataMode} />} />
          <Route path="/pipeline" element={<PipelinePage dataMode={dataMode} />} />
        </Routes>
      </Layout>
    </BrowserRouter>
  )
}
