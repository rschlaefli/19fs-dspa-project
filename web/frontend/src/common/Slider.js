import React from 'react'
import dayjs from 'dayjs'
import _range from 'lodash/range'

import { Slider } from 'antd'

export function SingleSlider({ currentTs, setCurrentTs, minTs, maxTs }) {
  // create an object of ticks for the slider
  const reducedTimestampObject = [
    ..._range(minTs, maxTs, 1800000),
    maxTs,
  ].reduce(
    (acc, mark) => ({
      ...acc,
      [mark]: dayjs(mark).format('HH:mm'),
    }),
    {}
  )

  return (
    <Slider
      included={false}
      min={minTs}
      max={maxTs}
      step={null}
      marks={reducedTimestampObject}
      value={currentTs}
      tipFormatter={value => dayjs(value).format('YYYY-MM-DD HH:mm:ss')}
      onChange={value => setCurrentTs(value)}
    />
  )
}

export function WindowSlider({
  currentTs,
  setCurrentTs,
  windowLength,
  minTs,
  maxTs,
}) {
  // compute the timestamp of the window start
  const windowStartTs = dayjs(currentTs).subtract(windowLength, 'hours')

  // create an object of ticks for the slider
  const reducedTimestampObject = [
    ..._range(minTs, maxTs, 3600000),
    maxTs,
  ].reduce(
    (acc, mark) => ({
      ...acc,
      [mark]: dayjs(mark)
        .add(1, 'hours')
        .format('HH'),
    }),
    {}
  )

  return (
    <Slider
      range
      min={minTs}
      max={maxTs}
      step={null}
      marks={reducedTimestampObject}
      value={[windowStartTs, currentTs]}
      tipFormatter={value => dayjs(value).format('YYYY-MM-DD HH:mm:ss')}
      onChange={value => setCurrentTs(value[1])}
    />
  )
}
