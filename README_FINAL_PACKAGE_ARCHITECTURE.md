HFA Architecture Manifest (Legacy Baseline)

Durum: Legacy / Historical Baseline

Amaç: Bu belge, HFA platformunun Sprint 1–6 arasında şekillenen ve Sprint 7 öncesi olgunlaşan mimari omurgasını, alınan kritik hardening kararlarını ve bu kararların gerekçelerini kayıt altına alır.

Bu belge current-state source of truth değildir. Özellikle Sprint 7 ile gelen persistence, telemetry ve SSE resilience değişiklikleri bu manifestin kapsamı dışındadır veya kısmen temsil edilir. Bu yüzden belge, üretim kararı için tek başına kullanılmamalı; Sprint 7 current-state dokümanı ve test sonuçlarıyla birlikte okunmalıdır.

1. Belgenin amacı ve kapsamı

Bu manifest şu üç amaca hizmet eder:

HFA’nın çekirdek mimari yönünü ve katman ayrımlarını tarihsel olarak belgelemek.

Sprint 1–6 boyunca kapatılan kritik riskleri ve neden bu kararların alındığını açıklamak.

Yeni sprintlerde yapılacak değerlendirmelerde “hangi temel kararlar sabit kaldı, hangileri evrildi” sorusuna referans üretmek.

Bu belge aşağıdaki alanları kapsar:

katmanlı mimari yaklaşımı

hfa-core / hfa-tools ayrımı

tenant isolation

governance omurgası

sandbox güvenlik ve çalıştırma modeli

orchestration öncesi ve orchestration ile uyumlu servis sınırları

repo hijyeni ve production hardening ilkeleri

Bu belge aşağıdaki alanlarda eksik veya tarihsel kabul edilmelidir:

Sprint 7 hibrit persistence gerçekliği

Redis-backed historical run lookup akışı

Inspector API’nin async history semantics’i

SSE heartbeat / disconnect / archived fallback davranışları

OpenTelemetry bootstrap / exporter wiring

durability / rehydration / replay / retention policy detayları

2. Mimari vizyonun özeti

HFA, dağınık prototip ajanlardan oluşan bir kod tabanı olmaktan çıkarılıp, tenant-aware, governance destekli, gözlemlenebilir ve kademeli olarak production’a yaklaştırılan çok ajanlı bir platform olarak ele alınmıştır.

Temel mimari yön şu ilkelere dayanır:

domain ve uygulama katmanları ayrılmalıdır

tenant sınırı açık ve doğrulanabilir olmalıdır

governance sonradan eklenen bir dekorasyon değil, akışın parçası olmalıdır

testler yalnızca doğruluk değil, operational safety de kanıtlamalıdır

yeşil test, production readiness ile eşit sayılmamalıdır

Bu yüzden HFA’nın evrimi boyunca tasarım kararları yalnızca “çalışıyor mu?” sorusuna göre değil; şu sorulara göre verilmiştir:

tenant isolation gerçekten korunuyor mu?

fail mode nedir?

cleanup sonrası state kaybı var mı?

async shutdown güvenli mi?

metrik sayacı ile gerçek durum arasında drift oluşur mu?

memory-only davranış operational risk doğurur mu?

3. Katmanlı mimari (Legacy baseline)

Aşağıdaki yapı, Sprint 1–6 boyunca fiilen şekillenen mimari omurgayı temsil eder.

Client / API
   |
   v
Middleware Layer
   |
   +--> Tenant isolation
   +--> Ledger / governance hooks
   +--> request context enrichment
   |
   v
Service Layer
   |
   +--> ArchitectService
   +--> ResearcherService
   +--> TesterService
   +--> Orchestrator (Sprint 5 ile belirginleşti)
   |
   v
Execution / Observability Layer
   |
   +--> ExecutionGraph
   +--> Run tracking
   +--> Inspector surface
   |
   v
Sandbox / Runtime Layer
   |
   +--> Code runner
   +--> SandboxPool
   +--> DistributedSandboxPool
   |
   v
Infra
   |
   +--> Redis
   +--> file-backed / process memory state
   +--> gelecekte kalıcı ops araçları

Bu omurganın amacı, ajan yeteneklerini doğrudan HTTP handler’larda değil, kontrol edilebilir servis sınırları içinde tutmaktır. Böylece tenant, budget, ledger, graph ve sandbox kontrolleri merkezi ve test edilebilir hale gelir.

4. Ana paket ayrımı
4.1 hfa-core

hfa-core, platformun domain ve altyapı omurgasını taşır. Burada amaç, uygulama yüzeyinden bağımsız olarak paylaşılan temel davranışları tanımlamaktır.

Başlıca sorumluluklar:

şemalar

governance

healing

LLM abstractions

observability primitives

ortak domain modelleri

Öne çıkan alanlar:

hfa.schemas.*

hfa.governance.budget_guard

hfa.governance.signed_ledger_v1

hfa.governance.compliance_policy

hfa.healing.store

hfa.obs.run_graph

Sprint 7 yönünde: hfa.obs.metrics

4.2 hfa-tools

hfa-tools, uygulama yüzeyi ve orchestration katmanını taşır. Burada amaç, HTTP/API, servisler, middleware ve sandbox altyapısı gibi çalışma zamanına daha yakın bileşenleri organize etmektir.

Başlıca sorumluluklar:

middleware

service orchestration

API endpoints

sandbox yönetimi

run registry / inspector yüzeyi

Öne çıkan alanlar:

hfa_tools.middleware.tenant

hfa_tools.middleware.ledger

hfa_tools.services.architect_service

hfa_tools.services.researcher_service

hfa_tools.services.tester_service

hfa_tools.services.orchestrator

hfa_tools.sandbox.runner

hfa_tools.sandbox.pool

hfa_tools.sandbox.distributed_pool

hfa_tools.api.inspector

Bu ayrım, Sprint 1–6 boyunca kritik bir repo hijyeni ve packaging düzeltmesi olarak öne çıkmıştır. Domain ile uygulama yüzeyinin ayrılması, import ve test kararlılığı için belirleyici olmuştur.

5. Legacy mimari bileşenleri
5.1 Tenant isolation

Tenant isolation, sistemin en temel güvenlik ve veri ayrımı eksenlerinden biridir.

Ana kararlar:

tenant header zorunlu olmalıdır

header erişimi case-insensitive uyumlu olmalıdır

resource ownership açıkça doğrulanmalıdır

run identifier canonical formatta üretilmelidir

nil UUID ve yanlış UUID versiyonları reddedilmelidir

tenant / run_id uyuşmazlığı fail-closed davranmalıdır

Legacy dönemde bu alanın ana sorunları şunlardı:

header/path extraction tutarsızlıkları

delimiter ve parsing riskleri

resource ownership doğrulamasındaki açıklar

test beklentisi ile gerçek hata mesajı ayrışması

Bu alan Sprint 1–4 arasında büyük ölçüde stabilize edilmiştir.

5.2 Budget governance

Budget hattı, maliyet kontrolünü operasyonel bir güvenlik meselesi olarak ele alır.

Temel hedefler:

atomic reserve/debit

commit / rollback güvenliği

idempotent davranış

fail-open / fail-closed tercihlerinin bilinçli belirlenmesi

testlerde gerçek Redis API davranışına yakınlık

Legacy hardening ile kapatılan başlıklar:

Lua script loading uyumu

evalsha çağrı şekli

mock ile gerçek API imzası arasındaki farklar

reset / freeze / recover akışları

atomic budget testlerinin yeşile çekilmesi

Budget hattı mimaride yalnızca maliyet sayacı değildir; agent execution akışında limit ve güvenlik katmanıdır.

5.3 Signed ledger

Signed ledger, auditability ve zincirlenmiş event kaydı için tasarlanmıştır.

Legacy hedefler:

tenant bazlı append-only kayıt

imzalı event zinciri

shutdown sırasında in-flight write güvenliği

çoklu node ortamında sessiz key üretiminin engellenmesi

Bu alanın production değeri, hata sonrası “ne oldu?” sorusuna sonradan cevap verebilmesidir. Ancak legacy belgede anlatılan dosya tabanlı model, sonraki fazlarda daha güçlü persistence mekanizmaları gerektirebilir.

5.4 Compliance policy

Compliance policy, findings üzerinde data-driven karar verme katmanıdır.

Temel ilkeler:

catch-all rule yasaklanmalıdır

init-time validation olmalıdır

kural değerlendirmesi açık semantik taşımalıdır

en kısıtlayıcı karar üstün gelmelidir

Bu katman, governance’ın “izin ver / HITL / deny” kararlarını şekillendirir.

5.5 Healing state store

Healing/state store katmanı, self-healing döngülerinin state takibi için tasarlanmıştır.

Legacy dönemde kritik kararlar:

soyut arayüz net olmalıdır

implement edilmemiş sınıflar instantiate edilememelidir

TTL davranışı test edilebilir olmalıdır

memory ve Redis store aynı contract çerçevesinde düşünülmelidir

Bu alan Sprint 7 persistence hattından farklıdır. Healing state ile run history aynı problem değildir; ancak her ikisi de “state’i yalnızca RAM’de bırakmama” prensibine bağlıdır.

5.6 Sandbox layer

Sandbox katmanı, HFA’nın güvenli ve tekrar üretilebilir code execution yüzeyidir.

Legacy hedefler:

container isolation

minimal privilege

network disable

tmpfs ile gerekli write alanları

stale cleanup

pool refill ve lifecycle yönetimi

Buradaki önemli öğrenimlerden biri şudur:

salt read_only=True güvenli görünse de, pratikte kodun çalışmasını bozuyorsa üretim için doğru tasarım değildir. Güvenlik ile çalışabilirlik arasındaki denge, gerçek test akışları üzerinden kurulmalıdır.

5.7 Service layer ve orchestration

Servis katmanı ilk başta agent-özel servisler olarak ortaya çıkmıştır:

architect

researcher

tester

sonraki ajanlar

Sprint 5 ile birlikte bu servis yüzeyleri daha belirgin bir orchestration omurgasına bağlanmıştır:

queue

worker

dispatch

concurrency limit

result storage

graph update

Bu geçiş, HFA’nın “tek tek servislerden” “koordine çalışan bir execution platformuna” evrilmesinde kritik eşiktir.

5.8 ExecutionGraph ve Inspector surface

ExecutionGraph ve inspector yüzeyi, platformun çalışma akışını görünür kılmak için eklenmiştir.

Legacy hedefler:

node start / commit / fail görünürlüğü

cost / token summary

run-level completion görünürlüğü

tenant’e göre graph erişimi

node filtreleme ve summary endpoint’leri

Sprint 6 itibarıyla bu alan memory tabanlı registry ile yeşile dönmüş, ancak persistence eksikliği açık teknik borç olarak kalmıştır.

6. Legacy repo yapısı

Aşağıdaki tree, gerçek dosya isimlerinden çok sorumluluk bölgelerini anlatan mantıksal bir legacy görünüm olarak okunmalıdır.

repo-root/
├─ hfa-core/
│  └─ src/hfa/
│     ├─ schemas/
│     ├─ governance/
│     ├─ healing/
│     ├─ llm/
│     ├─ obs/
│     └─ core/
│
├─ hfa-tools/
│  └─ src/hfa_tools/
│     ├─ api/
│     ├─ middleware/
│     ├─ sandbox/
│     └─ services/
│
├─ scripts/
└─ tests / reports / ops artifacts

Bu tree’nin en kritik mesajı şudur:

hfa-core paylaşılabilir domain/infra davranışlarını taşır

hfa-tools runtime ve API yüzeyini taşır

testler paket sınırlarına saygılı olmalıdır

duplicate test klasörleri ve archive testler aktif koleksiyona karışmamalıdır

7. Sprint 1–6 tarihsel evrim özeti
Sprint 1–4: foundation hardening

Bu dönem aşağıdaki alanlarda yoğunlaşmıştır:

tenant middleware doğruluğu

budget atomicity

architect test stabilitesi

repo packaging ve import hijyeni

sandbox baseline güvenilirliği

Bu evre sonunda temel çizgi şuydu:

tenant hattı ciddi biçimde sıkılaştı

budget guard testleri hizalandı

architect yürür hale geldi

repo test koleksiyonu temizlendi

sandbox tarafı baseline olarak kabul edilebilir seviyeye geldi

Sprint 5: orchestration + execution graph

Bu sprint ile sistemde queue, worker, run result ve graph takibi ilk kez omurgalı hale geldi.

Kazanımlar:

RunOrchestrator

concurrency control

queue full fast-fail

worker shutdown semantiği

active graph memory cleanup

wait_for / get_result

Ancak sprint yeşil olsa bile şu caveat’ler not edilmiştir:

ledger append fire-and-forget ise audit kaybı yaşanabilir

retention politikası belirsizdir

fairness ve backpressure gelecekte daha sert ele alınmalıdır

Sprint 6: event bus + distributed sandbox + inspector

Bu sprint, platformun gözlemlenebilir yüzeyini ve runtime dağıtım modelini güçlendirmiştir.

Kazanımlar:

in-memory event bus

tenant filter / wildcard davranışı

distributed sandbox pool

node registration / health / selection

run registry

inspector endpoint’leri

Ancak ana eksik açıktı:

registry memory-only idi

historical query persistence yoktu

SSE dayanıklılığı sonraki sprintte ele alınmalıydı

8. Legacy mimarinin güçlü yönleri
8.1 Net katman ayrımı

En önemli başarı, domain katmanı ile runtime/API katmanının ayrıştırılmasıdır. Bu ayrım, packaging sorunlarını azaltmış ve testlerin anlamlı sınırlar üzerinde kurulmasına izin vermiştir.

8.2 Tenant ve governance ciddiyeti

Tenant isolation ile budget / ledger gibi governance bileşenleri yüzeysel değil, akışın içine yerleştirilmiştir. Bu, HFA’yı sıradan demo-ajan yapılarından ayıran en önemli karakteristiklerden biridir.

8.3 Test kültürünün güçlenmesi

Sprintler ilerledikçe test yaklaşımı “ünite geçti” seviyesinden “operational risk var mı?” seviyesine evrilmiştir. Bu, özellikle sandbox, orchestrator ve registry alanlarında önemli bir olgunluk işaretidir.

8.4 Mimari omurganın oluşması

Sprint 6 sonunda HFA artık dağınık modüller toplamı değildir. Tenant, budget, sandbox, orchestration, graph ve inspector birbirine bağlanmış omurga oluşturmuştur.

9. Legacy mimarinin sınırlamaları

Bu belge ne kadar güçlü bir baseline sunsa da, aşağıdaki sınırlamalar Sprint 7 ile daha görünür hale gelmiştir.

9.1 Memory-only state kırılganlığı

Run registry memory tabanlı kaldığında:

process restart sonrası tarihçe kaybolur

inspector yalnızca canlı state’e bağlı kalır

completed run’lar cleanup ile yok olabilir

operational forensics zayıflar

9.2 Gözlemlenebilirlik yüzeyi eksikliği

Execution graph görünürlüğü vardır, fakat gerçek telemetry bootstrap yoksa sistem “görülebilir ama ölçülebilir değil” durumda kalır.

9.3 SSE dayanıklılığı eksikliği

Uzun ömürlü stream bağlantıları heartbeat, disconnect break ve archived fallback olmadan operational risk taşır.

9.4 Retention ve archive semantiği eksikliği

Run’ların ne kadar tutulacağı, nasıl archive edileceği, ne zaman silineceği net değilse sistem testte yeşil kalsa bile production’da kırılgan kalır.

9.5 Durability ve restart recovery eksikliği

Orchestrator queue/state memory’de kalıyorsa, cold restart sonrası in-flight run recovery yapılamaz.

10. Sprint 7 açısından bu manifestin eksikleri

Bu bölüm kritik önemdedir. Çünkü bu belge, Sprint 7 öncesi mimariyi iyi anlatsa da Sprint 7 current-state’i tam temsil etmez.

10.1 Hibrit persistence burada tam temsil edilmiyor

Sprint 7 Faz 1 ile gelen esas gerçeklik şudur:

aktif run’lar RAM’de tutulur

tamamlanan run’lar Redis’e archive edilir

lookup memory → Redis fallback ile yapılır

tenant run listesi active + historical merge ile oluşturulur

Legacy manifest bu davranışı ya hiç anlatmaz ya da memory-only registry mantığında kalır.

10.2 Inspector API artık async history-aware

Sprint 7 ile inspector endpoint’leri yalnızca canlı graph okuyucusu değildir. Historical snapshot okuma ve tenant doğrulamasını hibrit state üstünden yapar. Legacy manifest bu değişimi temsil etmez.

10.3 SSE artık yalnızca “stream” değildir

Sprint 7 Faz 3 ile beklenen davranış:

live graph varsa canlı takip

archived run ise single snapshot complete event

disconnect ise loop kırılır

heartbeat ile proxy timeout azaltılır

X-Accel-Buffering: no ile buffering önlenir

Legacy manifestte bu operational detaylar yoktur.

10.4 Telemetry yalnızca fikir değil, entegrasyon noktasıdır

Sprint 7 Faz 2 ile artık aşağıdaki entegrasyonlar önemlidir:

orchestrator enqueue/dequeue queue depth metric

run completion total/latency metric

distributed sandbox slot active metric

Legacy manifest observability’yi genel kavram olarak ele alır; metriklerin somut bağlandığı yerleri göstermez.

10.5 Bootstrap/exporter durumu belirsizdir

Metrik üretmek ile observability’nin operasyonel olarak hazır olması aynı şey değildir. Legacy manifest, Jaeger/Grafana gibi araçları sanki tam hazırmış gibi ima edebilir; oysa provider/exporter bootstrap yoksa bu aşırı iyimser olur.

10.6 Archive atomicity ve retention policy eksik

Sprint 7 current-state için aşağıdaki sorular kritik hale gelir:

Redis archive write atomic mi?

snapshot ve tenant index birlikte mi yazılıyor?

TTL / retention nasıl uygulanıyor?

cleanup / archive yarış penceresi var mı?

Legacy manifest bu sorulara cevap vermez.

10.7 Replay / reconnect / recovery semantiği tanımlı değil

SSE resilience eklense bile, daha ileri operational beklentiler şunlardır:

client reconnect sonrası replay var mı?

archived run için tekrar stream semantiği nedir?

orchestrator restart sonrası in-flight recovery planı var mı?

Bu alanlar legacy kapsam dışındadır.

11. Sprint 7 current-state için bu belge nasıl kullanılmalı

Bu belge aşağıdaki şekilde kullanılmalıdır:

Kullanılabilir

foundation hardening kararlarını hatırlatmak için

neden tenant/budget/sandbox/ledger kararları alındı açıklamak için

mimarinin tarihsel omurgasını korumak için

yeni tasarımların eski prensiplerle çelişip çelişmediğini değerlendirmek için

Tek başına kullanılmamalı

Sprint 7 production go/no-go kararı için

persistence readiness doğrulaması için

telemetry readiness doğrulaması için

SSE resilience doğrulaması için

current repo state belgesi olarak

12. Sprint 7 için açık eksikler ve yeniden yazılması gereken alanlar

Aşağıdaki başlıklar, Sprint 7 current-state dokümanında ayrıca tanımlanmalıdır.

12.1 RunRegistry persistence bölümü yeniden yazılmalı

Legacy yaklaşım yerine şu başlıklar açıkça yazılmalıdır:

live graph memory semantics

archive-on-cleanup behavior

Redis key schema

tenant history index strategy

active + historical summary merge semantics

sorting normalization

12.2 Inspector API bölümü yeniden yazılmalı

Yeni dokümanda şu endpoint davranışları açıkça yer almalıdır:

async snapshot lookup

tenant validation on historical data

memory fallback order

archived run graph response semantics

12.3 SSE bölümü baştan yazılmalı

Aşağıdaki maddeler current-state’e taşınmalıdır:

heartbeat cadence

disconnect break behavior

archived complete snapshot mode

proxy buffering headers

shutdown cancellation semantics

12.4 Observability bölümü genişletilmeli

Yalnızca “Grafana/Jaeger var” demek yeterli değildir. Şunlar somutlanmalıdır:

hangi metric nerede emit edilir

counter/histogram/up-down counter seçimi neden böyle

drift riskleri nelerdir

provider/exporter bootstrap durumu nedir

production scrape / export readiness var mı

12.5 Ops readiness bölümü dürüstleştirilmeli

Şu alanlar açık şekilde “tamamlanmadı” diye işaretlenmelidir:

durable queue

in-flight run recovery

cold restart rehydration

archive transactionality

retention/TTL enforcement

replay/reconnect semantics

13. Guardian değerlendirmesi

Bu manifest, HFA’nın omurgasını anlatmak için değerlidir; ama Sprint 7 sonrası current-state gerçeğini tek başına taşımaz.

Güçlü tarafı

projeye sadık

tarihsel kararları iyi açıklar

foundation hardening’i net taşır

mimari karakteri korur

Zayıf tarafı

persistence gerçekliğini eksik temsil eder

telemetry readiness’i olduğundan olgun gösterebilir

SSE resilience’i kapsam dışı bırakır

go/no-go dokümanı olarak fazla iyimser kalabilir

Guardian hükmü

Status: YELLOW

Bu belge, legacy architectural baseline olarak değerlidir. Ancak Sprint 7 ve sonrası için bunun yanına mutlaka ayrı bir current-state / production-readiness belgesi gerekir.

14. Sonuç

HFA’nın legacy mimarisi artık rastgele büyümüş bir ajan reposu değildir. Sprint 1–6 boyunca tenant isolation, governance, sandbox, orchestration ve observability yüzeyi belirgin şekilde güçlenmiştir. Bu nedenle bu manifest tarihsel olarak güçlü bir temel sunar.

Ancak Sprint 7 ile birlikte sistemin asıl kırılma noktası değişmiştir. Artık ana soru yalnızca “katmanlar doğru mu?” değildir. Yeni ana sorular şunlardır:

state kalıcı mı?

archive güvenli mi?

historical lookup doğru mu?

telemetry gerçekten dışarı export edilebilir mi?

SSE bağlantıları proxy ve disconnect koşullarında dayanıklı mı?

restart sonrası sistem ne kaybeder?

Bu yüzden bu belge korunmalıdır, fakat legacy olarak etiketlenmelidir. Sprint 7 değerlendirmeleri ve production kararı için ise ek, daha güncel ve operasyonel bir mimari belgesi zorunludur.

Oluşturdum.

Belge artık projeye sadık bir legacy baseline olarak yeniden yazıldı ve özellikle şu noktalar kapsamlı işlendi:

hfa-core / hfa-tools ayrımı

tenant / governance / sandbox / orchestration / inspector omurgası

Sprint 1–6 tarihsel evrim

legacy mimarinin güçlü ve zayıf tarafları

Sprint 7’de artık eksik kalan alanlar

hybrid persistence

async history-aware inspector

SSE heartbeat / disconnect / archived fallback

telemetry integration vs exporter/bootstrap farkı

retention / atomic archive / recovery semantics

Guardian hükmünü de belgeye koydum:

Status: YELLOW

legacy baseline olarak değerli

ama Sprint 7 current-state için tek başına yeterli değil

Bir sonraki doğru adım:
sprint7_current_state.md yazmak. Orada doğrudan şu başlıkları netleriz:

current Redis key model

RunRegistry live vs archived semantics

inspector async lookup contract

SSE behavior contract

metrics emission points

açık production riskleri

GREEN / YELLOW / RED sprint hükmü