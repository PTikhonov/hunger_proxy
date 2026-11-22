from __future__ import annotations
import asyncio, json, time, logging
from contextlib import asynccontextmanager
from typing import Any
import httpx
from fastapi import FastAPI, Form, HTTPException, Request, WebSocket, WebSocketDisconnect
from fastapi.responses import HTMLResponse, RedirectResponse
from itsdangerous import BadSignature, URLSafeTimedSerializer
from redis.asyncio import Redis
from redis.exceptions import ResponseError
from app.config import settings

logger = logging.getLogger("cashier-queue")

STYLE = '<style>body{font:16px system-ui;margin:0;background:#f6f7fb}.center{min-height:100vh;display:grid;place-items:center}.panel{background:#fff;padding:2rem;border-radius:12px;width:320px;box-shadow:0 8px 28px #0002}.panel input,.panel select,.panel button{box-sizing:border-box;width:100%;padding:.7rem;margin:.4rem 0}.top{display:flex;justify-content:space-between;align-items:center;margin:2rem}.card{background:white;padding:1rem;margin:.7rem 2rem;border-radius:10px}img{width:110px;vertical-align:middle;margin-right:1rem}</style>'
PAGE = '''<!doctype html><meta charset="utf-8"><title>Касса</title>{style}<div class=top><h1>Касса {cash}</h1><form method="post" action="/logout"><button type="submit">Выйти</button></form></div><div id=q>Загрузка…</div><script>function draw(j){q.innerHTML=j.queue.map(x=>`<div class=card>${x.image?'<img src="'+x.image+'">':''}<b>${x.person_id}</b>${x.offers.length?'<br><br><b>Офферы:</b><br>'+x.offers.map(o=>o.content).join('<br>'):''}<br><br><b>Интересы:</b><br>${x.interests.map(i=>i.name+' — '+i.total_seconds.toFixed(1)+' с').join('<br>')}</div>`).join('')||'<div class=card>Нет посетителей с интересами к категориям</div>'}async function refresh(){try{let r=await fetch('/api/queue',{cache:'no-store'});if(r.ok)draw(await r.json())}catch(_){}}refresh();setInterval(refresh,1000);let ws=new WebSocket((location.protocol==='https:'?'wss://':'ws://')+location.host+'/ws');ws.onmessage=e=>draw(JSON.parse(e.data))</script>'''
LOGIN = '''<!doctype html><meta charset="utf-8"><title>Вход кассира</title>{style}<main class=center><form class=panel method=post><h1>Вход кассира</h1><input name=username placeholder=Логин required autofocus><input name=password type=password placeholder=Пароль required><button>Войти</button>{error}</form></main>'''

@asynccontextmanager
async def lifespan(app: FastAPI):
    app.state.stream=Redis.from_url(settings.redis_stream_url,decode_responses=True); app.state.hot=Redis.from_url(settings.redis_hot_state_url,decode_responses=True); app.state.registry=Redis.from_url(settings.redis_registry_url,decode_responses=True)
    await app.state.stream.ping(); app.state.subscribers={}; app.state.cash_locations={}; app.state.worker=asyncio.create_task(worker(app)); yield
    app.state.worker.cancel(); await app.state.stream.aclose(); await app.state.hot.aclose(); await app.state.registry.aclose()
app=FastAPI(lifespan=lifespan); signer=URLSafeTimedSerializer(settings.session_secret, salt="cashier-session")

def session(request: Request)->dict[str,Any]:
    raw=request.cookies.get("cashier_session")
    try: return signer.loads(raw, max_age=8*3600) if raw else {}
    except BadSignature: return {}

@app.get('/login', response_class=HTMLResponse)
async def login_page(): return LOGIN.replace('{style}', STYLE).replace('{error}', '')
@app.post('/login', response_class=HTMLResponse)
async def login(username:str=Form(),password:str=Form(),request:Request=None):
    if not settings.hunger_head_url: raise HTTPException(503,'HUNGER_HEAD_URL is not configured')
    async with httpx.AsyncClient(timeout=5) as client:
        r=await client.post(settings.hunger_head_url.rstrip('/')+'/users/internal/cashier-login/',json={'username':username,'password':password},headers={'Authorization':'Bearer '+settings.hunger_head_token})
    if r.status_code!=200: return HTMLResponse(LOGIN.replace('{style}', STYLE).replace('{error}', '<p>Вход отклонён</p>'),status_code=401)
    data=r.json()
    response=RedirectResponse('/',303); response.set_cookie('cashier_session',signer.dumps(data),httponly=True,samesite='lax'); return response
@app.get('/',response_class=HTMLResponse)
async def page(request:Request):
    s=session(request)
    if not s: return RedirectResponse('/login')
    if not s.get('cash_register_id'):
        options=''.join(f'<option value="{x["id"]}">{x["number"]}</option>' for x in s.get('cash_registers', []))
        return HTMLResponse(f'<!doctype html>{STYLE}<main class=center><form class=panel method=post action=/select-register><h1>Выберите кассу</h1><select name=cash_register_id required>{options}</select><button>Открыть очередь</button></form></main>')
    return HTMLResponse(PAGE.replace('{style}', STYLE).replace('{cash}', str(s['cash_register'])))
@app.post('/select-register')
async def select_register(cash_register_id:str=Form(),request:Request=None):
    s=session(request)
    selected=next((x for x in s.get('cash_registers',[]) if str(x['id'])==cash_register_id),None)
    if not selected: raise HTTPException(403)
    s.update({'cash_register_id': selected['id'], 'cash_register': selected['number']})
    response=RedirectResponse('/',303); response.set_cookie('cashier_session',signer.dumps(s),httponly=True,samesite='lax'); return response
@app.post('/logout')
async def logout():
    response=RedirectResponse('/login',303)
    response.delete_cookie('cashier_session')
    return response
@app.get('/api/queue')
async def queue(request:Request):
    s=session(request)
    if not s: raise HTTPException(401)
    return await _state_with_interests(request.app, s)

async def _state_with_interests(app: FastAPI, s: dict[str,Any]) -> dict[str,Any]:
    raw=await app.state.hot.get('cashier-queue:'+str(s['cash_register_id']))
    state=json.loads(raw) if raw else {'queue':[]}
    rules=await app.state.registry.hget(f"hunger:promotion-rules:location:{s.get('location_id')}","payload")
    categories={str(r.get('category_id')): str(r.get('category_name') or r.get('category_id')) for r in json.loads(rules or '{}').get('rules',[]) if r.get('category_id')}
    visible=[]
    for row in state['queue']:
        row['offers'] = [offer for offer in row.get('offers', []) if str(offer.get('content') or '').strip()]
        fields=await app.state.hot.hgetall(row['person_id'])
        interests=[]
        for key,value in fields.items():
            if not key.startswith(f"interest-category:{s['location_id']}:") or not key.endswith(':total_seconds'): continue
            total=float(value or 0)
            if total>0:
                category_id=key.split(':')[2]; interests.append({'category_id':category_id,'name':categories.get(category_id, 'Категория '+category_id),'total_seconds':total})
        row['interests']=sorted(interests,key=lambda x:-x['total_seconds'])
        if not settings.show_images:
            row.pop('image', None)
        if row['interests']:
            visible.append(row)
    state['queue']=sorted(visible,key=lambda row: float(row.get('last_seen') or 0),reverse=True)
    return state
@app.websocket('/ws')
async def websocket_queue(websocket: WebSocket):
    raw=websocket.cookies.get('cashier_session')
    try: s=signer.loads(raw, max_age=8*3600) if raw else {}
    except BadSignature: s={}
    cash_id=str(s.get('cash_register_id') or '')
    if not cash_id:
        await websocket.close(code=4401); return
    await websocket.accept()
    websocket.app.state.subscribers.setdefault(cash_id,set()).add(websocket)
    websocket.app.state.cash_locations[cash_id]=s.get('location_id')
    try:
        await websocket.send_text(await _queue_payload(websocket.app, s))
        while True: await websocket.receive_text()
    except WebSocketDisconnect: pass
    finally: websocket.app.state.subscribers.get(cash_id,set()).discard(websocket)

async def _queue_payload(app: FastAPI, s: dict[str,Any]) -> str:
    return json.dumps(await _state_with_interests(app, s), ensure_ascii=True)

async def _publish(app: FastAPI, cash_id: str) -> None:
    payload=await _queue_payload(app, {'cash_register_id':cash_id, 'location_id':app.state.cash_locations.get(str(cash_id))})
    for socket in tuple(app.state.subscribers.get(str(cash_id),set())):
        try:
            # A disconnected browser must not hold up the Redis consumer.
            await asyncio.wait_for(socket.send_text(payload), timeout=1)
        except Exception:
            app.state.subscribers[str(cash_id)].discard(socket)

async def worker(app:FastAPI):
    """Keep the Redis consumer alive even if a transient Redis/WS error occurs."""
    while True:
        try:
            await _worker(app)
        except asyncio.CancelledError:
            raise
        except Exception:
            logger.exception("Cashier queue consumer stopped; restarting")
            await asyncio.sleep(0.2)

async def _worker(app:FastAPI):
    stream,hot,registry=app.state.stream,app.state.hot,app.state.registry
    try: await stream.xgroup_create(settings.observations_stream,settings.group,id='$',mkstream=True)
    except ResponseError: pass
    # Start at the beginning once: a customer may reach a cash camera after
    # an offer was generated at a shelf, so replay is needed to attach it.
    try: await hot.xgroup_create(settings.offers_stream,settings.group,id='0',mkstream=True)
    except ResponseError: pass
    while True:
        records=await _read_group(stream, settings.observations_stream)
        offer_records=await _read_group(hot, settings.offers_stream)
        records += offer_records
        mapping=json.loads((await registry.get(settings.camera_registry_key)) or '{}')
        for stream_name,messages in records:
            for mid,fields in messages:
                try:
                    payload=json.loads(fields.get('payload') or '{}')
                    if stream_name==settings.observations_stream:
                        camera=str(payload.get('camera_id') or ''); cash_id=mapping.get(camera)
                        if cash_id:
                            identity=str(payload.get('identity_id') or ''); ident=await hot.hgetall('identity:'+identity); person=str(ident.get('person_id') or identity)
                            key='cashier-queue:'+str(cash_id); state=json.loads((await hot.get(key)) or '{"queue":[]}'); now=time.time()
                            row=next((x for x in state['queue'] if x['person_id']==person),None)
                            if not row:
                                media=payload.get('media') or {}; norm=media.get('normalized') or media.get('multipart:normalized') or {}; row={'person_id':person,'image':norm.get('public_url') or norm.get('upload_url') or '','offers':[]}; state['queue'].append(row)
                            row['last_seen']=now; state['queue']=[x for x in state['queue'] if now-x.get('last_seen',now)<=settings.queue_ttl]; await hot.set(key,json.dumps(state),ex=settings.queue_ttl*2); await _publish(app, str(cash_id))
                    else:
                        person=str(payload.get('person_id') or ''); action=(payload.get('actions') or [{}])[0]
                        if action.get('delivery_type') != 'cashier_script' or not str(action.get('content') or '').strip():
                            continue
                        for cash in set(mapping.values()):
                            key='cashier-queue:'+str(cash); state=json.loads((await hot.get(key)) or '{"queue":[]}')
                            for row in state['queue']:
                                if row['person_id']==person and not any(offer['offer_id']==payload.get('offer_id') for offer in row['offers']): row['offers'].append({'offer_id':payload.get('offer_id'),'content':action['content']}); await hot.set(key,json.dumps(state),ex=settings.queue_ttl*2); await _publish(app, str(cash))
                except Exception:
                    # Bad data in one record must not prevent later visitors
                    # from reaching the cashier screen.
                    logger.exception("Failed to process stream event %s from %s", mid, stream_name)
                finally:
                    target = hot if stream_name == settings.offers_stream else stream
                    await target.xack(stream_name,settings.group,mid)

async def _read_group(redis: Redis, stream_name: str):
    claimed=await redis.xautoclaim(
        stream_name, settings.group, settings.consumer,
        min_idle_time=settings.pending_idle_ms, start_id='0-0', count=20,
    )
    if len(claimed) > 1 and claimed[1]:
        return [(stream_name, claimed[1])]
    return await redis.xreadgroup(
        settings.group, settings.consumer, {stream_name:'>'}, count=20, block=500,
    )
if __name__=='__main__':
 import uvicorn; uvicorn.run(app,host='0.0.0.0',port=8011)
