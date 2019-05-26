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
        <Tabs.TabPane tab="T1: Active Post Statistics" key={1}>
          <Statistics />
        </Tabs.TabPane>
        <Tabs.TabPane tab="T2: Recommendations" key={2}>
          <Recommendations />
        </Tabs.TabPane>
        <Tabs.TabPane tab="T3: Unusual Activity Detection" key={3}>
          <Anomalies />
        </Tabs.TabPane>
      </Tabs>
    </AppContainer>
  )
}

export default App
