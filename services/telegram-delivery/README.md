# telegram-delivery

Consumes `stream:offers` and sends only actions whose `delivery_type` is
`telegram`. Set `TELEGRAM_DELIVERY_ENABLED=true` in the hunger_proxy `.env`
before enabling it. The bot token and target chat id are read from the
corresponding Location in hunger-head, through the immutable promotion-rules
snapshot for the offer's `location_id` and `rules_revision`.

Successful actions are deduplicated by `offer_id` and `action_id`. Invalid
actions are sent to `stream:telegram-delivery-dead-letter`; transient Telegram
API failures remain pending for retry.
