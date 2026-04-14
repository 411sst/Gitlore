import { useState } from 'react'
import SetupView from './views/SetupView'
import IngestionView from './views/IngestionView'
import ChatView from './views/ChatView'

export default function App() {
  // view: 'setup' | 'ingestion' | 'chat'
  const [view, setView] = useState('setup')
  const [repoConfig, setRepoConfig] = useState(null)

  function handleIngestionStart(config) {
    setRepoConfig(config)
    setView('ingestion')
  }

  function handleIngestionComplete() {
    setView('chat')
  }

  function handleBackToSetup() {
    setView('setup')
    setRepoConfig(null)
  }

  return (
    <div className="min-h-screen bg-[#0D0D0D]">
      {view === 'setup' && (
        <SetupView onIngestionStart={handleIngestionStart} />
      )}
      {view === 'ingestion' && (
        <IngestionView
          repoConfig={repoConfig}
          onComplete={handleIngestionComplete}
          onBack={handleBackToSetup}
        />
      )}
      {view === 'chat' && (
        <ChatView
          repoConfig={repoConfig}
          onNewRepo={handleBackToSetup}
        />
      )}
    </div>
  )
}
