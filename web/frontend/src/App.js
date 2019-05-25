import React from 'react'
import styled from '@emotion/styled'

import { Tabs } from 'antd'

import Anomalies from './anomalies/Anomalies'
import Recommendations from './recommendations/Recommendations'
import Statistics from './statistics/Statistics'

const AppContainer = styled.div`
  padding: 1rem;
  padding-top: 0;
`

function App() {
  return (
    <AppContainer>
      <Tabs>
        <Tabs.TabPane tab="Active Post Statistics" key={1}>
          <Statistics />
        </Tabs.TabPane>
        <Tabs.TabPane tab="Friend Recommendations" key={2}>
          <Recommendations />
        </Tabs.TabPane>
        <Tabs.TabPane tab="Unusual Activities" key={3}>
          <Anomalies />
        </Tabs.TabPane>
      </Tabs>
    </AppContainer>
  )
}

export default App
