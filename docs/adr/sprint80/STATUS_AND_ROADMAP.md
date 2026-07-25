# Toprak1 — Durum Raporu ve Güncel Roadmap

- Belge durumu: CURRENT WORKING BASELINE
- Tarih: 25 Temmuz 2026
- Kapsam: Sprint 80 sonrası mimari ve ürün durumu
- Kaynak branch: `baseline/local-import`
- Kaynak HEAD: `3439618ea7ad8cf8bdd0e49d660217fc455c787c`
- Product implementation yetkisi: HAYIR
- Production-ready iddiası: HAYIR

## 1. Yönetici özeti

```yaml
repository_state: ACTIVE
current_base_branch: baseline/local-import
current_base_head: 3439618ea7ad8cf8bdd0e49d660217fc455c787c

product_runtime: FUNCTIONAL_BUT_NOT_PRODUCTION_READY
canonical_scheduler_composition: PRESENT
canonical_worker_composition: PRESENT
canonical_task_run_identity: PRESENT
real_redis_lua_execution_path: PRESENT
operator_evidence_and_cleanup_surfaces: PRESENT

sprint80b: FROZEN_AND_ACCEPTED
sprint80c: STARTED_ADR_DRAFTING
sprint80c_completed: false
accepted_architecture_decisions: 0
accepted_product_gaps: 15
product_implementation_started_after_80b: false
```

Ürün artık yalnız deneysel bir iskelet değildir. Scheduler ve worker için production composition root’ları, explicit task/run identity, gerçek Redis/Lua claim-complete hattı, completion sonrası ACK, crash/duplicate evidence yüzeyleri ve kontrollü operator cleanup sınırları mevcuttur.

Buna rağmen sistem production-ready değildir. Sprint 80B, repository gerçeğinde 15 açık product gap bulunduğunu executable evidence ile freeze etmiştir. Sprint 80C bu açıkları kapatmamış; yalnız karar verilmesi gereken mimari sözleşmeleri `PROPOSED` durumda tanımlamıştır.

Bugünkü en önemli gerçek şudur:

> Sıradaki çalışma product kodu yazmak değil, 80C mimari kararlarını kabul edilmiş ve uygulanabilir sözleşmelere dönüştürmektir.

## 2. Son doğrulanmış kilometre taşları

### Sprint 63–67 — Canonical execution hattı

Tamamlanan temel kabiliyetler:

- TaskConsumer seviyesinde fenced completion;
- completion commit edilmeden ACK verilmemesi;
- gerçek Redis/Lua WorkerConsumer → TaskConsumer bridge drill;
- read-only task evidence yüzeyi;
- tek komutla staging-benzeri runtime senaryosu.

### Sprint 68–75 — Crash ve terminal duplicate kontrol yüzeyleri

Tamamlanan kabiliyetler:

- crash-boundary evidence read model;
- terminal duplicate delivery’nin claim öncesi suppress edilmesi;
- explicit identity varsa güvenli ACK politikası;
- operator evidence;
- dry-run varsayılanlı kontrollü cleanup command;
- append-only cleanup audit trail;
- cleanup audit read model;
- task/run identity’nin fail-closed sertleştirilmesi.

### Sprint 76–79 — Production composition

Tamamlanan kabiliyetler:

- `task_id` ve `run_id` kimliklerinin bağımsız ve explicit taşınması;
- task-aware canonical scheduler dispatch writer;
- production scheduler composition root;
- production worker composition root;
- worker consumer’ın stream ACK authority olması;
- claim, heartbeat, completion ve shard lease fencing’in production graph içinde bağlanması;
- explicit production executor seçimi.

Son product-code doğrulaması PR #94 üzerinde raporlanmıştır:

```yaml
full_suite: 931_passed_2_skipped
focused_production_composition: 16_passed
executor_selection_contracts: 3_passed
compileall: PASS
git_diff_check: PASS
```

PR #95 ve PR #96 product runtime değiştirmemiştir. Bu nedenle product kodu PR #94’ten sonra semantik olarak değişmemiştir. Ancak mevcut `3439618...` HEAD üzerinde full product suite yeniden çalıştırılmış olarak ayrıca kanıtlanmış değildir; sonraki product implementation PR’ında current-base regression run zorunlu olmalıdır.

### Sprint 80B — Executable reconciliation ve evidence freeze

Durum:

```yaml
status: ACCEPTED_AND_FROZEN
source_head: 5734ac60704f8546b8ce67e766e042ff2ce4c412
artifact_sha256: 7b4e85209658619d400faebe3ee637580cbbebbb231c43531360ef3a79241588
finding_count: 15
reality_failures: 0
xpass: 0
unexpected_contract_failures: 0
```

80B sonuçları:

- reality suite: 87 passed, 6 skipped;
- frozen contracts: 3 passed, 17 expected XFAIL, 2 skipped;
- exact 12 mandatory evidence dosyası + `manifest.json`;
- branch, HEAD, size ve SHA-256 zinciri doğrulanmış evidence bundle;
- diagnostic scope dışında product mutation yok.

### Sprint 80C — ADR karar paketi

PR #96 ile `ADR-080C-decision-package.md` merge edilmiştir.

Güncel gerçek durum:

```yaml
adr_package_present: true
adr_status: PROPOSED
architecture_decisions_formally_accepted: 0
human_architecture_acceptance: NOT_RECORDED
product_implementation_authorized: false
sprint80c_closed: false
```

PR’ın merge edilmiş olması 80C’nin tamamlandığı anlamına gelmez. Merge edilen belge kararların kendisi değil, karar verilmesi gereken alanların taslağıdır.

## 3. Freeze edilmiş 15 product gap

### Event/state parity — 3

1. `event_first_orphan`
2. `state_first_missing_transport`
3. `committed_state_missing_background_audit`

### Canonical transition ve revision — 2

4. `no_verified_canonical_transition_record`
5. `no_aggregate_revision_evidence`

### Durability ve reconstruction — 6

6. `run_state_expires_without_reconstruction`
7. `run_result_expires_without_reconstruction`
8. `dag_task_state_expires`
9. `dag_task_meta_expires`
10. `task_output_expires`
11. `operator_audit_approximate_trim_only`

### Run/task truth — 2

12. `inconsistent_by_caller`
13. `contradictory_mutation_not_guaranteed_blocked`

### Parent/child correctness — 2

14. `missing_remaining_counter_unlocks_fail_open`
15. `ready_marker_strands_pending_child`

Bu gap’ler test arızası değildir. Mevcut product davranışının reality testleriyle gözlenmiş, target contract testlerinde strict XFAIL olarak korunan eksikleridir.

## 4. Mevcut risk değerlendirmesi

### P0 — Production claim öncesi zorunlu

- Tek ve deterministic runtime truth authority yok.
- Canonical transition record ve aggregate revision sözleşmesi yok.
- Event/state commit point ve atomicity modeli seçilmemiş.
- Expiring run/task authority kayıtları için kanıtlanmış reconstruction yolu yok.
- Parent completion içindeki child-effect aggregate sınırı kararsız.
- Contradictory cross-plane state mutation’ı bütün caller’larda fail-closed değil.

### P1 — Kontrollü rollout öncesi zorunlu

- Reconciliation/repair ownership ve operator workflow;
- transition deduplication ve replay politikası;
- legacy writer compatibility ve migration planı;
- shadow-read/shadow-write doğrulama modu;
- rollback ve data-compatibility sınırları;
- branch ruleset ile gerçek human approval enforcement.

### P2 — Takip borçları

- writer disposition identity’nin path yerine stable writer ID veya writer-set hash ile pinlenmesi;
- `final_summary.py --manifest-validated` için standalone manifest self-validation;
- curated event/state test ID’lerinin pytest output içinde generator tarafından doğrulanması;
- WorkerRegistry global list/count yollarındaki Redis `KEYS` kullanımı;
- bazı config alanlarındaki truthy-default normalization.

## 5. 80C kapanış planı — implementation öncesi zorunlu

80C yalnız aşağıdaki yedi kayıt formal olarak `ACCEPTED` olduğunda kapanabilir.

### 80C.1 — Runtime truth authority

Karar verilmesi gerekenler:

- task lifecycle için authoritative record;
- run lifecycle için authoritative record;
- read/query, admission, dispatch, claim, heartbeat, completion ve recovery caller matrisi;
- conflict, stale ve missing behavior;
- terminal truth’in mutation bloklama kuralı;
- reconciliation owner.

Önerilen yön:

- task lifecycle işlemlerinde TASK truth authoritative;
- run lifecycle işlemlerinde RUN truth authoritative;
- cross-plane conflict mutation için fail-closed;
- conflict durumunda explicit reconciliation queue/read model.

### 80C.2 — Aggregate boundary

Kesin seçim yapılmalıdır:

- bağımsız task aggregate + process manager; veya
- parent/child etkilerini içeren DAG/run aggregate.

Her iki durumda da değişmez invariant:

> Child effect içeren her committed aggregate revision, child effect’leri taşıyan tam bir adet CanonicalTransitionRecord üretmelidir.

### 80C.3 — Canonical transition/revision contract

Kabul edilmesi gereken minimum alanlar:

- transition identity;
- aggregate type/id;
- monotonic aggregate revision;
- operation taxonomy;
- previous/next state;
- child effects;
- causation/correlation/idempotency identities;
- authoritative timestamp;
- stable writer ID;
- schema version.

Ayrıca duplicate, retry, revision conflict ve projection application davranışı kararlaştırılmalıdır.

### 80C.4 — Durability/reconstruction authority

Her state family tam bir sınıfa atanmalıdır:

- durable authority;
- reconstructable projection;
- ephemeral coordination;
- retained execution cache;
- audit history;
- transport retention.

“Reconstructable” etiketi yalnız şu kanıtlarla kabul edilmelidir:

- durable source;
- executable recovery algorithm;
- tested loss window;
- operator-visible failure;
- recovery owner.

### 80C.5 — Event/state atomicity

Her transition class için commit modeli seçilmelidir:

- Redis Lua atomic mutation + canonical record;
- transactional outbox;
- tek authority write + projection emission;
- açıkça gerekçelendirilmiş başka model.

Commit point, audit failure davranışı, orphan detection ve projection repair kuralları explicit olmalıdır.

### 80C.6 — Writer identity ve feature-flag governance

Karar şunları içermelidir:

- stable writer ID registry;
- writer-to-operation allowlist;
- composition-root registration;
- production default behavior;
- feature flag owner ve sunset/revisit trigger;
- enforcement katmanı.

### 80C.7 — Implementation sequencing ve rollback

Kabul edilen mimarinin:

- sprint sırası;
- compatibility sınırı;
- migration modu;
- rollout ölçütleri;
- rollback trigger’ları;
- old/new writer coexistence süresi;
- evidence ve acceptance gate’leri

tek bir uygulanabilir plana bağlanması gerekir.

### 80C closure gates

```yaml
immutable_80b_input_identified: PASS
all_15_findings_mapped_to_decisions: REQUIRED
accepted_decision_records: 7
unresolved_architecture_choice: 0
product_source_mutation: 0
implementation_claims: 0
verification_contracts_defined: REQUIRED
owning_implementation_sprints_assigned: REQUIRED
human_architecture_acceptance: REQUIRED
```

## 6. Önerilen implementation roadmap

Aşağıdaki sprintler `PROPOSED` durumdadır. 80C kabul edilmeden başlamamalıdır.

### Sprint 81 — Canonical transition foundation

Amaç:

- accepted schema’yı code contract olarak eklemek;
- stable writer registry ve operation taxonomy oluşturmak;
- aggregate revision conflict davranışını fail-closed tanımlamak;
- mevcut runtime’a henüz geniş migration yapmadan canonical record writer’ın vertical slice’ını kurmak.

Exit gate:

- bir seçilmiş transition path için exactly-one canonical record;
- duplicate retry’de aynı idempotency identity;
- revision conflict mutation yapmıyor;
- legacy transport/audit behavior bozulmuyor.

### Sprint 82 — Task truth authority convergence

Amaç:

- scheduler, modern worker, control read model ve task recovery caller’larını accepted TASK authority sözleşmesine geçirmek;
- cross-plane contradiction mutation’ını fail-closed yapmak;
- reconciliation observation üretmek.

Exit gate:

- 80B truth contract XFAIL’leri PASS;
- dokuz caller observation tek deterministic authority matrisine uyuyor;
- conflict sırasında product mutation sıfır.

### Sprint 83 — Atomic transition commit model

Amaç:

- accepted commit point’i task admit/dispatch/claim/complete vertical slice’larında uygulamak;
- canonical record ile authority mutation arasındaki atomicity’yi sağlamak;
- missing projection repair mekanizmasını eklemek.

Exit gate:

- event-first orphan yok;
- state-first missing transport authority kaybı yok;
- audit failure’ın business transition etkisi accepted contract’a uyuyor;
- parity XFAIL’leri PASS.

### Sprint 84 — Parent/child aggregate correctness

Amaç:

- accepted aggregate modelini parent completion’a uygulamak;
- missing counter’ın fail-open unlock üretmesini engellemek;
- ready marker semantiğini idempotent yapmak;
- child effects’i canonical record’a dahil etmek.

Exit gate:

- iki parent/child XFAIL PASS;
- duplicate parent completion child’i iki kez unlock etmiyor;
- missing/corrupt dependency state fail-closed ve repairable.

### Sprint 85 — Durability ve reconstruction

Amaç:

- authority/projection TTL politikasını accepted ADR’ye göre düzeltmek;
- run state/result, task state/meta/output için durable source veya executable reconstruction sağlamak;
- operator audit retention/archival sözleşmesini uygulamak.

Exit gate:

- altı durability gap için executable recovery evidence;
- recovery sonrası authority ve projections tutarlı;
- loss window ölçülmüş ve raporlanmış.

### Sprint 86 — Reconciliation ve controlled repair

Amaç:

- contradiction/orphan/missing projection tespiti;
- read-only diagnosis;
- explicit operator command sınırı;
- audit-intent-before-mutation;
- idempotent repair.

Exit gate:

- otomatik sessiz repair yok;
- her repair canonical transition/audit evidence taşıyor;
- retry güvenli;
- rollback veya manual escalation yolu mevcut.

### Sprint 87 — Compatibility ve shadow migration

Amaç:

- legacy writer/readers için compatibility adapters;
- shadow canonical record üretimi veya shadow read comparison;
- writer-set pinning;
- feature flag rollout planı;
- mismatch telemetry.

Exit gate:

- shadow mismatch kabul edilen eşik altında;
- old/new path karşılaştırma artifact’i;
- rollback tek flag veya açık deployment adımıyla mümkün;
- unknown production writer sıfır.

### Sprint 88 — Chaos, restart ve soak evidence

Amaç:

- process crash boundary’leri;
- Redis restart/failover;
- duplicate delivery;
- delayed projection;
- worker/scheduler leadership handoff;
- reconstruction ve repair drill’leri.

Exit gate:

- deterministic evidence artifacts;
- no silent data loss;
- no contradictory committed mutation;
- bounded recovery time;
- operator-visible degraded state.

### Sprint 89 — Production-readiness decision gate

Amaç:

- bütün accepted contracts’ın executable gate’lerini toplamak;
- operational runbook, dashboards, alerts ve rollback doğrulaması;
- upgrade/migration dry-run;
- final independent audit.

Exit gate:

```yaml
accepted_80b_gaps_remaining: 0
unexpected_xfail: 0
xpass_without_review: 0
full_current_head_suite: PASS
real_redis_runtime_suite: PASS
restart_and_recovery_drills: PASS
operator_repair_drills: PASS
migration_and_rollback_drill: PASS
human_release_decision: REQUIRED
production_ready_claim: DECISION_REQUIRED
```

## 7. Bağımlılık ağacı

```text
80B evidence freeze — COMPLETE
        |
        v
80C accepted architecture decisions — CURRENT BLOCKER
        |
        v
81 canonical transition foundation
        |
        +--> 82 truth authority convergence
        |
        +--> 83 atomic transition commit
        |
        +--> 84 aggregate child correctness
                    |
                    v
85 durability and reconstruction
        |
        v
86 reconciliation and controlled repair
        |
        v
87 compatibility and shadow migration
        |
        v
88 chaos/restart/soak evidence
        |
        v
89 production-readiness decision
```

82, 83 ve 84 implementation olarak kısmen paralelleştirilebilir; fakat aynı canonical record/revision contract’a bağlı olmalıdır. Ayrı ve çelişkili writer modelleri üretilmemelidir.

## 8. Yönetişim düzeltmeleri

PR #95 merge sürecinde human freeze kararı GitHub tarafından enforce edilmemiş ve owner kararı post-merge kaydedilmiştir. Bu evidence’i bozmaz; fakat süreç kontrolünün yalnız belge içinde bulunmasının yeterli olmadığını göstermiştir.

Implementation PR’larından önce `baseline/local-import` ruleset’inde hedeflenen kontroller:

- en az bir approving review;
- PR sahibinin kendi approval’ının yeterli sayılmaması;
- admin bypass’ın kapatılması veya sınırlanması;
- Authority Gate required check;
- ilgili sprint/full-suite check’lerinin required olması;
- unresolved conversation varken merge engeli;
- draft PR merge engeli;
- merge sonrası target-branch validation veya merge-queue kullanımı.

Bu kontroller kod doğruluğunun yerine geçmez; yalnız kabul edilen sequence’in GitHub tarafından gerçekten uygulanmasını sağlar.

## 9. Kesin sınırlar

Aşağıdakiler yapılmamalıdır:

- 80C accepted olmadan Sprint 81 product implementation’a başlanması;
- current product için production-ready denmesi;
- full Event Sourcing veya CQRS’nin varsayılan çözüm olarak dayatılması;
- bütün Redis key’lerinin tek seferde yeniden tasarlanması;
- legacy yolların evidence olmadan topluca silinmesi;
- XFAIL’lerin yalnız marker kaldırılarak “çözülmesi”;
- recovery’nin yalnız belge üzerinde reconstructable ilan edilmesi;
- merge edilmiş ADR taslağının accepted architecture kararı sanılması.

## 10. Sıradaki tek doğru çalışma

```yaml
next_phase: SPRINT_80C_DECISION_CLOSURE
product_code_changes_allowed: false
first_decisions_to_resolve:
  - runtime_truth_authority
  - aggregate_boundary
  - canonical_transition_revision_contract
required_output:
  - seven_accepted_ADR_records
  - fifteen_finding_to_decision_matrix
  - verification_contracts
  - implementation_sprint_ownership
  - human_architecture_acceptance
```

İlk çalışma paketi, 80C.1 ve 80C.2 için caller/aggregate decision matrix’lerini tamamlamalı; ardından 80C.3 canonical transition contract’ı dondurulmalıdır. Bu üç karar çözülmeden durability veya atomicity implementation’ına başlanması yeniden architecture theater ve paralel truth üretme riski taşır.
