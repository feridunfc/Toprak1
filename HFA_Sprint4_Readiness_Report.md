# HFA V5.3 IRONCLAD — Sprint 4 Readiness Report

## Executive Summary

Bu rapor, kamuya açık GitHub görünümü ile bu sohbette paylaşılan dosyaların birlikte incelenmesine dayanır.
Ana sonuç: proje **ilerlemiş**, ancak **Sprint 4 tamamlandı / production-ready** demek için henüz erken. En büyük sorun, **tek bir kanonik kontrat yerine birden fazla yarışan kontratın** aynı repoda yaşamasıdır:

- `hfa-tools/tests/test_sprint2.py` hâlâ eski Sprint 2 beklentilerinin bir bölümünü taşıyor.
- `hfa-tools/tests/test_budget_guard_atomic.py` daha yeni, 3-script / 4-field idempotent dönüş kontratına yakın.
- `tenant.py` dosyasının açıklama bloğu ile gerçek implementasyonu tam hizalı değil.
- `rate_limit.py` AsyncMock/pipeline uyumluluğunda runtime warning üretmeye açık.
- Repo kökünde hem `tests/` hem `hfa-tools/tests/` bulunması test stratejisini karıştırıyor.

## Repo Snapshot

Kamuya açık GitHub görünümünde:
- varsayılan branch `main`
- görünür branch sayısı: yalnızca `main`
- görünür commit geçmişi: 8 commit
- kök ağaçta `.idea`, `hfa-core`, `hfa-tools`, `scripts`, `tests`, `.gitignore` klasör/dosyaları görünüyor.

## Uploaded Files Reviewed

Bu raporda özellikle şu dosyalar incelendi:
- `tenant.py`
- `rate_limit.py`
- `ledger.py`
- `agent.py`
- `research.py`
- `supervisor.py`
- `test_sprint2.py`
- `test_tenant_patches.py`
- `test_budget_guard_atomic.py`
- ayrıca önceki `hfa_pre_sprint4_fixes_v2.zip`

## Current State by Area

### 1) BudgetGuard

**İyi taraflar**
- Atomic odaklı yeni test paketi mevcut: `test_budget_guard_atomic.py`
- Bu paket 3 script, unpacked `evalsha`, `run_ids` idempotency seti, `recover_run`, `fail_open/fail_closed` ve pipeline uyumluluğu gibi daha güncel beklentileri testliyor.

**Sorunlar**
- `test_sprint2.py` içinde hâlâ 4 script beklentisi var.
- Aynı repo içinde bir test paketi 4-script kontrat beklerken diğeri 3-script kontrat bekliyor.
- Bu durum üretim kodunu değil, **test sözleşmesini** belirsiz hale getiriyor.

**Karar**
- Kanonik kontrat olarak **3-script atomic contract** seçilmeli.
- `hfa-tools/tests/test_budget_guard_atomic.py` tutulmalı.
- `hfa-tools/tests/test_sprint2.py` içindeki BudgetGuard bölümü ya güncellenmeli ya da kaldırılmalı.

### 2) Tenant Middleware

**İyi taraflar**
- Dosyada gerekli public export'ların çoğu var:
  `TenantContext`, `extract_tenant_from_resource_id`,
  `assert_tenant_owns_resource`, `is_valid_tenant_id`,
  `validate_run_id_format`, `TenantMissingError`, `TenantMismatchError`,
  `get_tenant_context`.
- Exact segment ownership kontrolü korunmuş.
- Middleware 400/403 ayrımını dönüyor.

**Sorunlar**
- Dosya başındaki patch açıklaması `_header_get()` kullanımını vaat ediyor; fakat `_extract_tenant()` içinde hâlâ doğrudan `request.headers.get(...)` kullanılıyor.
- Bu nedenle plain `dict` test doubles ile header case-insensitive davranış garantisi zayıf.
- `validate_run_id_format()` şu an UUID parse ediyor ama **UUIDv4 zorunluluğunu uygulamıyor**.
- Nil UUID guard da görünmüyor.
- `test_sprint2.py` içindeki `run-acme_corp-00000001` gibi örnekler yeni entropy/UUID kuralıyla zaten uyumsuz.

**Karar**
- Tenant için kanonik test seti `test_tenant_patches.py` olmalı.
- `test_sprint2.py` içindeki tenant bölümü ya aynı kontrata taşınmalı ya da kaldırılmalı.

### 3) Rate Limiting

**İyi taraflar**
- `rl:<tenant_id>:<minute_bucket>` key formatı doğru yönde.
- 120 saniye TTL yaklaşımı makul.
- `fail_open=False -> 503`, `fail_open=True -> allow` modeli doğru.

**Sorunlar**
- `_increment()` içinde:
  - `pipe.incr(key, 1)`
  - `pipe.expire(key, 120)`
  sync çağrı varsayımıyla yazılmış.
- AsyncMock ile test edildiğinde bu iki çağrı await edilmediği için runtime warning üretmeye açık.
- BudgetGuard’daki `_maybe_await` yaklaşımı burada da standardize edilmeli.

**Karar**
- `rate_limit.py` production için kalmalı.
- Ama `_maybe_await` veya eşdeğer helper ile pipeline queue metotları normalize edilmeli.

### 4) Ledger Middleware

**İyi taraflar**
- `asyncio.get_running_loop().create_task(...)` kullanımı doğru.
- Fire-and-forget yazım modeli uygun.
- Hata yutma davranışı HTTP response’u koruyor.

**Riskler**
- Ledger dependency bulunamazsa sadece loglayıp geçiyor; bu kabul edilebilir ama startup health check ile güçlendirilmeli.
- `run_id = tenant_ctx.run_id or f"unscoped-{tenant_ctx.tenant_id}"` fallback’i audit açısından kabul edilebilir ama policy ile netleştirilmeli.

**Karar**
- Bu dosya Sprint 4 öncesi tutulmalı; küçük hardening yeterli.

### 5) Schemas (`agent.py`, `research.py`, `supervisor.py`)

**İyi taraflar**
- `agent.py` içinde `Literal` import edilmiş.
- `TypeAdapter` + discriminated union yapısı kurulmuş.
- `supervisor.py` içinde `BudgetPolicy`, `ComplianceRule`, `RoutingPolicy`, `SupervisorPolicy` ayrımı iyi.
- `research.py` ayrıştırılmış ve daha temiz.

**Sorunlar**
- `agent.py` içinde duplicate typing import var.
- `run_id` regex'i generic ID gibi; tenant middleware’deki gerçek `run-<tenant>-<uuid>` kontratını birebir yansıtmıyor.
- Schema-level run_id regex ile middleware-level run_id policy arasında contract drift var.

**Karar**
- Schema paketi tutulmalı.
- Ancak `run_id` validation tek merkezli hale getirilmeli.

## Keep / Remove / Merge

### Keep
- `hfa-tools/tests/test_budget_guard_atomic.py`
- `hfa-tools/tests/test_tenant_patches.py`
- `hfa-tools/src/hfa_tools/middleware/ledger.py`
- `hfa-tools/src/hfa_tools/middleware/rate_limit.py` (refactor sonrası)
- `hfa-core/src/hfa/schemas/supervisor.py`
- `hfa-core/src/hfa/schemas/research.py`

### Merge / Refactor
- `hfa-core/src/hfa/schemas/agent.py`
- `hfa-tools/src/hfa_tools/middleware/tenant.py`
- `hfa-tools/tests/test_sprint2.py`

### Remove or Archive
- `hfa-tools/tests/test_sprint2.py` içindeki:
  - eski BudgetGuard beklentileri
  - tenant için UUID'siz / entropy'siz örnekler
  - rate limit ile çakışan eski beklentiler
- repo root altındaki duplicate `tests/` kopyaları, eğer `hfa-tools/tests/` ile aynı amacı taşıyorsa arşivlenmeli.

## Canonical Test Layout Recommendation

- `hfa-core/tests/` → core governance/schema/healing testleri
- `hfa-tools/tests/` → middleware/service/sandbox HTTP-seviye testleri
- repo root `tests/` → sadece integration/e2e/orchestration testleri

Önerilen sonuç:
- unit testler component’e yakın yaşasın
- sprint regression testleri tek yerde olsun
- aynı kontratı iki farklı dosya farklı şekilde test etmesin

## Production Blockers Before Sprint 4 Done

1. **Tenant contract drift**
   - `_header_get()` gerçekten tüm extraction path'lerinde kullanılmalı.
   - `validate_run_id_format()` UUIDv4 + nil guard uygulamalı.
   - testler tek kontrata çekilmeli.

2. **BudgetGuard contract drift**
   - 3-script contract resmen seçilmeli.
   - `test_sprint2.py` eski beklentilerden temizlenmeli.

3. **Rate limit async warning**
   - pipeline queue metotları `_maybe_await` ile normalize edilmeli.

4. **Duplicate tests**
   - `tests/` ve `hfa-tools/tests/` ayrımı netleştirilmeli.

5. **Dependency and packaging sanity**
   - editable install ile `hfa_tools` ve `hfa` importları CI’da doğrulanmalı.
   - tek `pytest.ini` / tek marker policy belirlenmeli.

## Sprint 4 Completion Plan

### Phase A — Contract Freeze
1. BudgetGuard canonical contract = 3 scripts + atomic debit result `[allowed, spent, status, idempotent]`
2. Tenant canonical contract = case-insensitive header lookup + exact segment owner check + UUIDv4 only
3. Rate limit canonical contract = fixed window + `_maybe_await` safe pipeline

### Phase B — Test Cleanup
1. `test_sprint2.py` sadece gerçekten sprint regression olan testleri tutsun
2. Eski BudgetGuard beklentileri kaldır
3. Tenant UUID'siz örnekleri kaldır
4. Duplicate root tests arşivle / taşı

### Phase C — Code Hardening
1. `tenant.py`
   - `_extract_tenant()` içinde `_header_get()` kullan
   - `validate_run_id_format()` → UUIDv4 + nil UUID guard
   - `TenantContext` injection ve response header propagation testlerini hizala
2. `rate_limit.py`
   - `_maybe_await` ekle
   - `pipe.incr/expire` uyumlulaştır
3. `agent.py`
   - duplicate import temizliği
   - `run_id` field policy’sini tenant contract ile hizala

### Phase D — CI Gate
Minimum green set:
- `hfa-tools/tests/test_budget_guard_atomic.py`
- `hfa-tools/tests/test_tenant_patches.py`
- `hfa-tools/tests/test_execute_architect.py`
- `hfa-tools/tests/test_sandbox.py`
- curated `hfa-tools/tests/test_sprint2.py`

### Phase E — Sprint 4 Deliverables
Sprint 4 bitti denebilmesi için:
- Debugger service green
- Healing loop state machine green
- circuit breaker green
- compliance ordering tests green
- tenant/budget/rate-limit/ledger middleware contractları frozen

## Final Assessment

Bugünkü durumda proje:
- **Sprint 1:** mostly stable
- **Sprint 2:** functionally advanced but test-contract fragmented
- **Sprint 3:** partially integrated
- **Sprint 4:** **başlanabilir**, ama “bitmiş” demek için önce contract cleanup şart

Kısa hüküm:
**Kod tabanı umut verici, ama production-ready olmak için birleştirilmiş test sözleşmesi ve tek kanonik middleware/budget kontratı gerekiyor.**
