(ns aeonik.inventory.store)

(defprotocol InventoryStore
  (start-scan! [store scan-document]
    "Persist a scan in running state before any observations are accepted.")
  (write-observations! [store scan-id observations]
    "Persist one bounded batch of observation documents.")
  (complete-scan! [store scan-id completion]
    "Mark a successfully persisted scan complete.")
  (fail-scan! [store scan-id failure]
    "Mark an interrupted or failed scan without claiming complete coverage."))
