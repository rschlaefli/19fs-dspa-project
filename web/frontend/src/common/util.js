import _sortBy from 'lodash/sortBy'
import dayjs from 'dayjs'

import { SortDirection } from 'react-virtualized'

export function sortItems(rows, sortBy, sortDirection) {
  let sortedRows
  if (Array.isArray(sortBy)) {
    sortedRows = _sortBy(rows, sortBy)
  } else {
    sortedRows = _sortBy(rows, [sortBy])
  }

  // reverse if descending is expected
  if (sortDirection === SortDirection.DESC) {
    return sortedRows.reverse()
  }

  // return the sorted rows
  return sortedRows
}

export function formatTimestamp(timestamp) {
  return dayjs(timestamp)
    .add(1, 'seconds')
    .format('YYYY-MM-DD HH:mm:ss')
}

export function formatObjectAsString(object) {
  return Object.entries(object)
    .filter(([key, value]) => key !== '__typename' && value != null)
    .map(([key, value]) => `${key}=${value}`)
    .join(', ')
}
