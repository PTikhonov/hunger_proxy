# offer-generator

Consumes `stream:interest-threshold-events`, reads the immutable promotion-rules
snapshot referenced by `rule_revision`, chooses one eligible promotion, and emits
an offer with all of that promotion's Actions to `stream:offers`.

It does not call Xibo, Telegram, a cash-register WebSocket, or an AI service.
Those integrations consume `stream:offers` later.  `offer-generator` is
idempotent by person, location, category, selected context, threshold, and rules
revision; the Redis `SET NX` and stream append are one Lua operation.
