# xibo-audio-delivery

Consumes `stream:offers` and processes only actions with
`delivery_type: xibo_audio`.  The action's `layout_id` is used as the Xibo
layout id. For legacy offers, `content` may contain `123` or `123&Name`.
Exactly one destination is required: `player_group_id` or
`player_id`.

It follows the Xibo CMS flow used by the legacy proxy: obtain an OAuth token
from `POST /api/authorize/access_token`, then replace the target's layout with
`POST /api/displaygroup/{id}/action/changeLayout`. The deployed CMS exposes
this operation for display groups; configure a group target for such a CMS.

The preferred credential source is `delivery.xibo` in the immutable
promotion-rules snapshot:

```json
{"xibo": {"cms_url": "https://cms.example", "client_id": "...", "client_secret": "..."}}
```

Until hunger-head publishes that field, the service can use `XIBO_CMS_URL`,
`XIBO_CLIENT_ID`, and `XIBO_CLIENT_SECRET` from its environment.  Credentials
are never copied into an offer.
