# k8s-alert: первая версия

Статус: согласован пользователем; первая реализация выполнена 2026-11-22.
Фактическая конфигурация и ограничения — README.md и charts/k8s-alert/values.yaml.

## Цель и согласованные требования

Лёгкий Go-сервис внутри Kubernetes, устанавливаемый Helm-чартом. Отправляет
уведомления о выбранных авариях в Пачку через webhook и в Telegram через Bot API.
Все следующие возможности обязательны уже в первой версии:

- Отличать аварийное завершение от штатного удаления/замены пода.
- `env-type: dev|prod`: dev игнорирует единичные аварийные рестарты,
  prod уведомляет о первом наблюдаемом аварийном завершении.
- Получать логи контейнера; количество строк задаётся конфигурацией;
  в JSON есть отдельное поле `logs`.
- Include/exclude по namespace, имени пода и kind, включая полное
  исключение Job/CronJob и принадлежащих им подов.
- Хранить состояние проблемы и подавлять повторные уведомления по
  настраиваемому интервалу, в том числе при замене пода Deployment.
- Удобный `values.yaml`, Secret для webhook, инструкции установки.
- Telegram в первой версии: bot token из Secret, chat_id, опциональный
  message_thread_id для темы. Telegram опционален и по умолчанию отключён;
  его Secret и chat_id при отключении не требуются. Пачка и Telegram могут
  работать одновременно после явного включения.

Параметры и детали ниже, не заданные пользователем, — предложения автора.

## Рассмотренные подходы

1. Доработать alertkube: готовая доставка и эксплуатационные функции,
   но требуется менять владельцев, ключи дедупликации и семантику рестартов.
2. Собственный Go-сервис с client-go: больше начальной работы, зато компактная
   реализация и полностью управляемое поведение. Пользователь выбрал его.

От kwatch/alertkube берём принципы: informers, отдельная очередь доставки,
ограниченный сбор диагностики, сохранение состояния. Код проектов не копируем.

## Границы первой версии

Мониторим Pods (включая обычные init-контейнеры), Deployments, ReplicaSets,
StatefulSets, DaemonSets, Jobs и CronJobs. Последние два также нужны для
разрешения владельцев, даже когда их алерты отключены.
Не добавляем UI, CRD, Prometheus-зависимость, внешнюю БД, анализ ошибок по
тексту логов, облачные интеграции, автоматическое исправление и поиск первопричины.
Логи собираем при сбое, не держим непрерывный stream для каждого контейнера.

## Обнаружение и штатные операции

Источник истины — состояния/conditions ресурсов; Events дают контекст.
Watch не гарантирует наблюдение каждого очень короткого промежуточного состояния.

1. Контейнер: используем текущее и последнее termination, restartCount,
   containerID, finishedAt, причины, exit code и signal. Изменение счётчика
   само по себе не доказывает аварию. Код 137 сам по себе не доказывает OOM.
2. Классы: ContainerCrash (ненулевой exit), OOMKilled, ProbeFailure,
   CrashLoop, ImagePullFailure, ContainerConfigFailure, PodPending,
   PodNotReady, WorkloadUnavailable, RolloutFailed, ReplicaSetFailedCreate.
   Причины BackOff/CrashLoop и аварийные exit одного контейнера объединяются
   в ContainerCrash; OOM — отдельный класс.
3. Удаление пода, штатный rollout/scale-down, завершение Job, exit 0 и
   ожидаемый SIGTERM при удалении не создают container-crash алерт.
   Не подавляем настоящую аварию нового пода лишь потому, что идёт rollout.
   OOM, завершившийся до начала удаления, не теряется из-за последующего удаления.
4. Недоступность/ошибки запуска проверяются после `for`, отдельно от рестартов.
   Штатный rollout не алертит, пока сохраняется требуемая доступность;
   потеря доступности сверх `for` или ProgressDeadlineExceeded алертит.
5. ReplicaSetFailedCreate детектируется по condition ReplicaFailure и
   Warning Events FailedCreate: Pod может ещё не существовать.
6. Для обычных init-контейнеров проверяем неуспешный exit и waiting-состояния;
   завершение init с кодом 0 нормально. Native sidecars учитываем отдельно
   от одноразовых init-контейнеров; завершение при остановке пода штатно.

На первом sync уже работающего пода не выдаём прошлый restartCount за новые
падения. Текущие активные ошибки проверяем после sync; недоступность API или
несинхронизированный cache означает unknown и не считается восстановлением.
Ненаблюдавшиеся падения во время простоя сервиса нельзя надёжно восстановить.

## Профили и переопределения

`env-type` задаётся на экземпляр сервиса/кластер. Настройки правил можно
переопределять для namespace, labels, pod/workload name и workload kind.
Профиль задаёт defaults, явные параметры имеют приоритет.

- prod: первое новое аварийное завершение сразу открывает инцидент.
- dev: ContainerCrash/OOM/ProbeFailure открываются после 3 различных
  аварийных завершений одного контейнера workload за 5 минут.
- dev: одиночное краткое CrashLoopBackOff не обходит порог; устойчивый
  CrashLoop открывает инцидент после 2 минут даже без наблюдённых 3 exit.
- Оба профиля: Pending 240s, NotReady 120s, image/config failure 60s,
  workload unavailable 120s; явный RolloutFailed/FailedCreate после 30s.
- Повтор 30m, восстановление после 2m стабильного здоровья.

Количество падений считается по уникальным termination, не по одному и тому
же объекту на resync. Для workload считаем разные завершения по контейнеру
в пределах окна; несколько реплик могут вместе достигнуть порога.

## Владельцы и фильтры

Следуем ownerReference с controller=true: Pod→ReplicaSet→Deployment,
Pod→Job→CronJob, Pod→StatefulSet/DaemonSet, либо самостоятельный Pod/ReplicaSet/Job.
Идентичность включает apiGroup/kind/namespace/name/UID. Отсутствие права или
владельца не заменяется догадкой по имени; цепочка помечается incomplete.
При неизвестном владельце и настроенных kind-фильтрах повторяем разрешение;
не отправляем потенциально исключённый Pod до разрешения, учитываем в метриках.

Глобальные exclude имеют приоритет. Пустой include означает все. Поля
одного match соединены AND; элементы списка одного поля — OR. Имена
поддерживают glob, labels — Kubernetes label selector. Kind matcher
проверяет сам объект и всю controller-цепочку. Исключение Job/CronJob
подавляет и прямые события, и контейнерные алерты их подов.
Правила переопределения упорядочены: первое совпавшее задаёт overrides,
остальное наследуется из defaults. Конфигурация строго валидируется.

## Инциденты и отсутствие спама

Ключ termination: Pod UID + container name + containerID/finishedAt.
Ключ инцидента: cluster + namespace + root workload UID (или Pod UID без
владельца) + container (если применимо) + стабильный failure class.
Замена Pod/ReplicaSet одного Deployment сохраняет ключ; пересоздание самого
Deployment с новым UID создаёт новую идентичность.
`notifications.group-by: workload|pod` позволяет выбрать гранулярность,
default workload. При pod ключ включает Pod UID, поэтому новый под получает
собственный таймаут. В обоих вариантах разные контейнеры/классы независимы.

Состояния: pending → firing → recovering → resolved; unknown при потере
наблюдения не закрывает инцидент. После первого успешного уведомления
повторы не чаще repeat-interval, с количеством новых падений и затронутых
подов. Напоминания запускает таймер, а не только приход очередного Event.
Рестарты между backoff не означают восстановление: нужен период Ready без
новых аварий. Удаление одного пода workload не закрывает весь инцидент.
Для самостоятельного удалённого пода причина закрытия — resource_deleted,
а не ложное утверждение о восстановлении приложения.
Предлагаемый default `notifications.recovery: true`: отправляем отдельный
resolved webhook только если по инциденту уже успешно доставлен firing.
Это распространяется на resource_deleted с явным текстом «ресурс удалён».
При recovery=false закрываем внутреннее состояние без сообщения.

Логи берём до подавления уведомления; другие API-проверки и отправку
выполняем с ограниченной конкурентностью. Одно падение не создаёт сразу
отдельные Error, BackOff и HighRestartCount сообщения. В первой версии
разные классы (например OOM и FailedCreate) могут быть отдельными инцидентами.

## Логи и payload

Получаем последние N строк упавшего контейнера через pods/log. Для нового
запуска после рестарта используем previous=true; если контейнер ещё
terminated и не перезапущен — текущие логи. Если источник недоступен,
допускаем диагностический fallback с явным source, без утверждения, что
это точно логи конкретного termination. Удалённые/ротированные логи могут
быть недоступны — алерт всё равно отправляется.

По умолчанию: 50 строк, 16 KiB на контейнер, не более 3 подов в одном
алерте, общий payload не более 64 KiB, таймаут логов 5s. UTF-8 не разрываем.
Токены и пароли маскируем известными шаблонами; поддерживаем пользовательские
redaction regexp. Диагностика не содержит webhook URL/Secret.

Версионированный JSON: schema_version, incident_id, status, cluster,
env_type, namespace, resource, workload, container, reason, failure_class,
exit_code, restart_count, first_seen, last_seen, occurrences, message,
logs (массив объектов pod/container/source/text/truncated/error), events.

Generic webhook получает этот JSON. Пачка получает тот же объект плюс
`message` со сформированным текстом и блоком логов; отдельное `logs`
сохраняется для шаблонов входящего webhook. Текст ограничиваем 24 KiB.
На incoming webhook Пачки не требуется токен бота: достаточно Secret с URL.
Получатели задаются списком `destinations` с уникальными name и type:
`pachca`, `telegram`, `generic`. Отправляем во все enabled получатели.
Telegram использует HTTPS Bot API sendMessage, не входящий webhook;
параметры chat-id (строка), опциональный message-thread-id и token из
existingSecret/token-key. Не нужны polling, getUpdates или входящий HTTP.
Бот должен иметь доступ к целевому чату. Каноническое поле logs сохраняется
в webhook JSON; для Telegram формируем текст с логами, максимум 4096 символов
по правилам Bot API, сокращая сначала логи с явной отметкой. Не разделяем
один алерт на множество сообщений. Используем plain text, без parse_mode.
Bot token не выводится в ошибки/URL логов; проверяем HTTP и поле ok в ответе.
Telegram 429 учитывает parameters.retry_after. Доставка, retries, успешный
send и repeat-interval учитываются отдельно для каждого получателя:
сбой Telegram не повторяет уже доставленное сообщение Пачки.
После изменения конфигурации и рестарта при добавлении получателя открытый
инцидент доставляется ему один раз; восстановленные задания удалённых или
отключённых получателей отменяются. Изменение
адреса/token у прежнего name не сбрасывает историю успешных отправок.
См. https://dev.pachca.com/guides/incoming-webhooks.
См. https://core.telegram.org/bots/api#sendmessage.

## Доставка и сохранение

Bounded очередь 256 сообщений, отдельные workers, HTTP timeout 10s.
Повторяем network/408/429/5xx с backoff+jitter, учитываем Retry-After;
остальные 4xx постоянные ошибки, видны в логах/метриках. Максимальный возраст
доставки 1h. Repeat-интервал начинается после успешной доставки, а не попытки.
Один notification_id сохраняется при retry. Гарантия at-least-once:
при сбое после приёма webhook и до checkpoint возможен дубль.

Состояние инцидентов, окон падений и компактный outbox сохраняем в одном
собственном ConfigMap: gzip + base64, период 10s и graceful shutdown.
Это checkpoint с возможной потерей последних 10s при аварийном завершении,
не строгая durable-очередь. Payload для retry содержит урезанные логи.
Сжатое состояние ограничено 700 KiB; сначала удаляем resolved-историю и
логи из outbox, затем старые напоминания. Переполнение активных/первичных
алертов отражаем в метриках и error log, не маскируем под успешное сохранение.
Без PVC, внешней БД и абсолютных гарантий доставки при переполнении.
Если ConfigMap недоступен, продолжаем мониторинг в памяти с метрикой degraded.
Первая версия — строго одна реплика, Deployment strategy Recreate,
валидация replicaCount=1 в Helm; HA за пределами первой версии.

## Helm и эксплуатация

`charts/k8s-alert`: values.schema.json, README и values с комментариями;
profiles dev/prod, ConfigMap, ServiceAccount, минимальный ClusterRole/Binding,
Deployment, Service, опциональный NetworkPolicy. Secrets через existingSecret
и key, URL читается из файла; права чтения всех Secret не требуются.
RBAC: get/list/watch для перечисленных ресурсов и Events; get pods/log;
get/create/update только собственного state ConfigMap, где возможно
resourceNames (create ограничивается namespace отдельным Role).
Default excludes пусты: включённые Job/CronJob исключаются явной настройкой.

Конфигурация и credentials читаются и строго валидируются только на старте.
Встроенного hot reload, polling файла и atomic config swap нет.
Изменения ConfigMap и Secret применяются перезапуском через Stakater Reloader,
установленный отдельно в кластере. Chart не устанавливает второй Reloader.
При `reloader.enabled: true` chart добавляет на metadata Deployment
`configmap.reloader.stakater.com/reload` с именем только конфигурационного
ConfigMap и `secret.reloader.stakater.com/reload` с именами Secret включённых
получателей. State ConfigMap исключён: его регулярные записи не должны
вызывать цикл рестартов. Generic auto-аннотацию не используем.
При отключённой интеграции config checksum в pod template обеспечивает
рестарт на helm upgrade; внешняя ротация Secret требует ручного рестарта.
Если Reloader включён, checksum динамической конфигурации отключён, чтобы
не запускать второй rollout вслед за Reloader. Изменение image, mounts,
RBAC и других полей pod template может вызвать обычный Helm rollout.
Новый процесс восстанавливает историю и очередь из state ConfigMap,
пересчитывает pending timers и напоминания по новым настройкам, сохраняя
идентичность проблем и timestamps успешных отправок. Инциденты, ставшие
исключёнными по новым фильтрам, удаляются из активных без ложного recovered.
Изменённые через kubectl edit значения могут перезаписаться следующим
helm upgrade; source of truth — values в Git.

Health: /healthz процесс; /readyz синхронизация обязательных caches;
/metrics: incidents, suppressed, delivery failures/retries, queue depth,
log failures, state failures. Нет внешнего входящего API.
Контейнер non-root, readOnlyRootFilesystem, drop capabilities, без hostPath.
Начальные requests 25m/64Mi, limits 200m/128Mi — предварительные значения,
которые проверяем на dev; расход памяти зависит от числа объектов.

## Проверка и поставка

Целевой вид `values.yaml` (пример интерфейса, не реализованный chart):

```yaml
replicaCount: 1
config:
  version: 1
  cluster: dev
  env-type: dev
  filters:
    namespaces:
      include: []
      exclude: [kube-system]
    pod-names:
      include: []
      exclude: []
    kinds:
      exclude: [Job, CronJob]
  detectors:
    container-crash: {enabled: true}
    oom-killed: {enabled: true}
    probe-failure: {enabled: true}
    crash-loop: {enabled: true, for: 2m}
    image-pull-failure: {enabled: true, for: 60s}
    container-config-failure: {enabled: true, for: 60s}
    pod-pending: {enabled: true, for: 240s}
    pod-not-ready: {enabled: true, for: 120s}
    workload-unavailable: {enabled: true, for: 120s}
    rollout-failed: {enabled: true, for: 30s}
    replicaset-failed-create: {enabled: true, for: 30s}
  # Отсутствующий restarts.threshold наследуется из профиля:
  # dev=3, prod=1. Ниже можно явно переопределить.
  restarts:
    window: 5m
  notifications:
    group-by: workload
    repeat-interval: 30m
    recovery: true
    recovery-stable-for: 2m
  logs:
    enabled: true
    tail-lines: 50
    max-bytes: 16384
    max-pods: 3
    timeout: 5s
  overrides:
    - match:
        namespaces: [sandbox]
        workload-names: [experimental-*]
      restarts:
        threshold: 5
      notifications:
        repeat-interval: 1h
destinations:
  - name: team-pachca
    type: pachca
    enabled: true
    existingSecret: k8s-alert-pachca
    url-key: url
  - name: team-telegram
    type: telegram
    enabled: false
    existingSecret: k8s-alert-telegram
    token-key: bot-token
    chat-id: "-1001234567890"
    # message-thread-id: 42
reloader:
  enabled: true
persistence:
  enabled: true
```

`detectors.*.enabled` отключает соответствующие классы независимо от профиля;
профиль влияет на пороги, а не включает обратно отключённый detector.
Для одного физического сбоя уведомление формируется по наиболее конкретному
включённому классу. Точная кодовая нормализация классов покрывается тестами.

Тестируем поведение fake client + fake clock + HTTP test server: одиночное
падение dev/prod; окно рестартов; planned deletion; OOM; CrashLoop между
запусками; init error; Job/CronJob descendants; смена пода; независимость
контейнеров; таймер повторов; подтверждённое восстановление; unknown;
недоступные логи; восстановление после рестарта с новым config;
точечные Reloader-аннотации без наблюдения state ConfigMap;
отсутствие Telegram Secret и отправок при enabled=false;
частичные ошибки multi-destination доставки, Telegram limits/ok/retry_after;
429/5xx; восстановление checkpoint и лимиты.
Проверки: go test -race, go vet, helm lint, helm template для dev/prod,
values schema и RBAC. При доступном тестовом кластере smoke через kind.

Поставка: исходники, Dockerfile, Helm chart, документация, пример payload,
проверочные сценарии и точные команды установки/удаления. Реальная публикация
образа и helm install потребуют registry, kube-context и Secret webhook;
их значения пока не предоставлены. Не утверждаем, что сервис выкатили без
фактической проверки Deployment и уведомления.

## Свобода реализации

Автор реализации выбирает актуальную совместимую версию Go/client-go,
конкретную YAML/metrics библиотеку, внутренние структуры и имена пакетов.
Конфигурация имеет version: 1; неизвестные поля и невалидные durations
отклоняются с понятным путём поля. Пороги выше могут быть изменены после
dev-обратной связи, все обязательные функции остаются в первой поставке.


## Уточнения реализации

- Проверка восстановления: отдельные свежие списки API каждые 15s только при
  наличии инцидентов; ошибка или устаревший результат сбрасывает healthy clock.
- При превышении checkpoint 700 KiB сначала убираем сохранённые логи/events;
  если размер всё ещё велик, оставляем прошлый checkpoint и degraded metric.
- Постоянная ошибка доставки firing удерживает следующие попытки до repeat
  interval, но не считается успешной отправкой и не порождает resolved.
- First release assets существуют локально в dist; GitHub/GHCR ещё не опубликованы.
