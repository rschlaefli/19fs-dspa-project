import React from 'react'

import { Button, InputNumber } from 'antd'

function PollIntervalControl({ pollInterval, setPollInterval }) {
  return (
    <>
      <Button
        icon="sync"
        onClick={() => setPollInterval(pollInterval ? undefined : 4000)}
      >
        {pollInterval ? 'Stop Refetch' : 'Enable Refetch'}
      </Button>
      <InputNumber
        formatter={value => `${value / 1000} seconds`}
        min={1000}
        max={10000}
        step={1000}
        value={pollInterval}
        onChange={value => setPollInterval(value)}
      />
    </>
  )
}

export default PollIntervalControl
