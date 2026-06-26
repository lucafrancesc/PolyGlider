package com.polyglider.consumer

object Topology {
  val MainExchange          = "orders.exchange"
  val MainQueue             = "orders.placed"
  val DlxExchange           = "dlx.orders.exchange"
  val DlxQueue              = "dlx.orders.placed"
  val NeedsAttentionExchange = "needs-attention.exchange"
  val NeedsAttentionQueue   = "needs-attention.orders.placed"
}
