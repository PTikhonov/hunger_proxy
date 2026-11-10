# xibo-video-delivery

This Compose service runs the same Xibo delivery worker as
`xibo-audio-delivery`, configured with `XIBO_DELIVERY_TYPE=xibo_video` and its
own consumer group, deduplication keys and target cooldown keys.  Keeping the
worker image shared ensures that audio and video use the same CMS contract
without duplicating delivery logic.

See [`../xibo-audio-delivery/README.md`](../xibo-audio-delivery/README.md) for
the Xibo request contract and credential configuration.
