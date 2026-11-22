# k8s-alert

Небольшой Go-сервис для уведомлений о сбоях Kubernetes. Пачка через incoming
webhook, опционально Telegram Bot API и произвольный JSON webhook. Установка Helm.

**Состояние:** версия 0.1.6 подготовлена к dev-проверке. Команды OCI ниже
заработают после публикации пакетов этой версии.
Go и Docker на машине установки не нужны.

## Быстрая установка

Нужны `kubectl`, Helm и доступ к кластеру. Stakater Reloader устанавливается
отдельно; если он уже работает в кластере, дополнительных действий не требуется.

Создайте Secret с URL входящего webhook Пачки (источник «Свой формат», без
шаблона используется поле `message`). При наличии готового Secret укажите его
имя и ключ в `destinations` файла values.

```sh
kubectl create namespace k8s-alert
kubectl -n k8s-alert create secret generic k8s-alert-pachca \
  --from-literal=url='https://api.pachca.com/webhooks/REPLACE_ME'

helm upgrade --install k8s-alert oci://ghcr.io/dgeliko/charts/k8s-alert \
  --version 0.1.6 -n k8s-alert --create-namespace \
  --set config.cluster=dev --set config.env-type=dev --wait --timeout 3m
```

Команда Secret содержит пример: настоящий URL не публикуйте в Git. Чтобы не
оставлять значение в истории команд, используйте `--from-file=url=/path/to/url`.

Для установки из checkout вместо OCI-адреса используйте `./charts/k8s-alert`.
Всё поведение задаётся [values.yaml](charts/k8s-alert/values.yaml),
с готовыми профилями [dev](examples/dev.yaml) и [prod](examples/prod.yaml):

```sh
helm upgrade --install k8s-alert ./charts/k8s-alert \
  -n k8s-alert --create-namespace -f examples/prod.yaml --wait
kubectl -n k8s-alert logs deployment/k8s-alert
```

Имя контейнерного пакета: `ghcr.io/dgeliko/k8s-alert:v0.1.6`. Оба GHCR-пакета
(образ и chart) должны иметь Public visibility,
если кластер будет скачивать их без авторизации. Для Private-образа задайте
`imagePullSecrets`; для Private-chart выполните `helm registry login ghcr.io`.

## Что вызывает уведомление

- Аварийное завершение контейнера, включая init-контейнеры, OOM и CrashLoopBackOff.
- Ошибки загрузки образа и создания конфигурации контейнера.
- Длительный Pending/NotReady, недоступность workload, сорванный rollout.
- Ошибка ReplicaSet FailedCreate, даже если Pod не удалось создать.
- Liveness-причина добавляется, когда SIGKILL подтверждён недавним probe Event.

Штатное завершение с exit 0, ожидаемый SIGTERM при удалении и остановка native
sidecar при успешном завершении Job не считаются падением. Код 137 сам по себе
не означает OOM. Rollout не подавляет реальные ошибки новых контейнеров.

`env-type: prod` сообщает о первом новом аварийном завершении; `dev` по
умолчанию требует 3 разных завершения за 5 минут. Устойчивый CrashLoopBackOff
дополнительно обнаруживается через 2 минуты. Пороги и классы отключаются/меняются
в values. На старте исторические рестарты уже работающего контейнера не
выдаются за новые аварии.

Повторы по умолчанию раз в 30 минут. Ключ проблемы — workload UID + контейнер
+ класс сбоя. Новый Pod того же Deployment не сбрасывает интервал; другой
контейнер или новый UID самого Deployment — отдельная проблема. Для режима
по подам задайте `config.notifications.group-by: pod`.

Восстановление — после 2 минут стабильной работы, подтверждаемой свежими
состояниями API. При открытых инцидентах сервис раз в 15 секунд дополнительно
считывает ресурсы для проверки восстановления. Потеря наблюдения не считается
восстановлением. Удаление самостоятельного ресурса обозначается как удаление.

## Фильтры и исключения

```yaml
config:
  filters:
    namespaces:
      include: [team-*]
      exclude: [team-sandbox]
    pod-names:
      include: [api-*]
      exclude: [api-test-*]
    kinds:
      exclude: [Job, CronJob]
    label-selector: "app.kubernetes.io/part-of=crm"
```

Имена поддерживают glob, labels — стандартный Kubernetes selector.
Exclude всегда приоритетнее include; пустой include означает все.
Чтобы ограничить размер кэша и проверку восстановления, перечислите точные
имена в `namespaces.include`. Пустой список или glob требуют наблюдения за
всем кластером, а фильтр применяется при обработке.
Kind проверяется по controller-цепочке, например Pod → Job → CronJob,
поэтому исключение CronJob распространяется на его поды.
Исключение по имени пода применяется к подам; workload-уведомления не имеют
имени пода, используйте namespace/labels/kind для фильтрации всех уровней.
При незавершённой цепочке владельцев и настроенном kind-фильтре уведомление
ждёт разрешения владельца, вместо догадки по имени.

Переопределения используют первое совпавшее правило:

```yaml
config:
  overrides:
    - match:
        namespaces: [sandbox]
        workload-names: [experimental-*]
      restarts: {threshold: 5, window: 10m}
      notifications: {repeat-interval: 1h}
```

## Логи и payload

`config.logs.tail-lines` — число строк (по умолчанию 15), `max-bytes` — предел на
контейнер (16 KiB), `max-pods` — число образцов подов (3), `timeout` — 5s.
Логи собираются по событиям, в том числе до достижения dev-порога.
Для перезапущенного контейнера читаем previous logs, для ещё завершённого —
current. Удалённые/ротированные логи могут быть недоступны; алерт всё равно
отправляется с `logs[].error`. Постоянного анализа текста всех логов нет.

JSON webhook содержит отдельный массив `logs`; пример —
[examples/payload.json](examples/payload.json). В Пачке `message` дополнительно
содержит читаемый текст с логами. Для шаблона Liquid доступно поле
`logs_json` с готовым JSON: каждый фрагмент содержит Pod, контейнер, источник
и массив строк `lines`. Число строк задаётся через `config.logs.tail-lines`.
В Telegram отправляется текст до 4096
символов. Большие данные сокращаются с пометкой; размер webhook JSON до 64 KiB.
Известные токены/пароли, Bearer и значения credentials получателей маскируются.
Пользовательские regexp задаются в `config.logs.redact-patterns`.

## Telegram (опционально)

По умолчанию выключен: не требует Telegram Secret и не выполняет отправок.
Для включения создайте Secret с bot token, добавьте бота в чат, укажите
`chat-id` и при необходимости `message-thread-id` в [examples/telegram.yaml](examples/telegram.yaml).

```sh
kubectl -n k8s-alert create secret generic k8s-alert-telegram \
  --from-file=bot-token=/path/to/bot-token
helm upgrade --install k8s-alert ./charts/k8s-alert -n k8s-alert \
  -f examples/prod.yaml -f examples/telegram.yaml
```

Списки Helm заменяются целиком: при включении Telegram сохраняйте запись
Пачки в destinations, если хотите оба канала. Доставка и подавление повторов
независимы: ошибка одного канала не повторяет успешную отправку в другой.

## Конфигурация, состояние и рестарты

Встроенного hot reload нет. Конфигурация и credentials читаются на старте;
Stakater Reloader перезапускает Deployment при изменении config/Secret.
При `reloader.enabled: true` и `reloader.auto: true` чарт добавляет `reloader.stakater.com/auto: "true"`
в Deployment. Reloader следит за ConfigMap и Secret, на которые ссылается
Pod. Служебный ConfigMap состояния приложение читает через Kubernetes API,
поэтому запись состояния не вызывает перезапуск.
При `reloader.auto: false` Reloader следит только за ConfigMap конфигурации
и Secret включённых получателей.
С `reloader.enabled: false` Helm checksum перезапускает Pod при config upgrade;
изменение внешнего Secret в этом режиме требует `kubectl rollout restart`.

Одна реплика, стратегия Recreate. Состояние инцидентов и очередь доставки
сохраняются в собственном ConfigMap каждые 10s и при graceful shutdown.
При аварии возможна потеря последних секунд checkpoint и повтор сообщения.
HTTP retry — network/408/429/5xx, backoff+jitter до 1h, с учётом Retry-After.
Постоянные ошибки видны в логах/метриках; это не гарантия exactly-once.
Checkpoint ограничен 700 KiB: сначала убираются логи из сохранённой очереди,
при дальнейшей нехватке остаётся прежний checkpoint и выставляется degraded.
Без PVC и внешней БД. Удаление релиза не удаляет созданный приложением state
ConfigMap; для полного сброса удалите `<release>-state` вручную.

Health: `/healthz`, `/readyz`, `/metrics`, порт 8080 (внутри кластера).
Сервису не нужны pods/exec, доступ к значениям всех Secret или запись workloads.

## Разработка и сборка

Go 1.26+; текущий Docker builder и CI используют Go 1.27.4.

```sh
go mod download
make test
make chart
make build
```

Для ограниченной консоли используйте writable caches:

```sh
GOCACHE="$PWD/.cache/go-build" GOMODCACHE="$PWD/.cache/go-mod" go test -race ./...
```

На Mac ARM64 локальная сборка даёт macOS-бинарник. Для Linux:

```sh
make release-binaries VERSION=v0.1.6
# dist/k8s-alert-linux-amd64 и dist/k8s-alert-linux-arm64

docker buildx build --platform linux/amd64 --load \
  --build-arg VERSION=v0.1.6 -t k8s-alert:v0.1.6 .
# Для публикации обеих архитектур:
make image VERSION=v0.1.6
```

В Dockerfile GOOS/GOARCH берутся из TARGETOS/TARGETARCH; host Mac не задаёт
архитектуру кластера. Multi-arch tag автоматически выбирает нужный вариант.

## Публикация

Откройте GitHub → **Actions → Release → Run workflow**, выберите `main` и
укажите новую версию, например `v0.1.6`. Тег создаст сам workflow после
успешной сборки. Можно также запустить релиз обычным push тега `v*.*.*`.
Существующий релиз не перезаписывается. Для запуска через подключённый GitHub
без браузера/консоли поддерживается создание ветки `release/v0.1.6` от
проверенного коммита: событие create запускает тот же Release workflow.
Обычные feature-ветки релиз не запускают.

Перед запуском релиза проверьте успешный CI для выбранного коммита. Release
проверяет версию, собирает артефакты и публикует:

- контейнер `ghcr.io/dgeliko/k8s-alert:v0.1.6`, linux/amd64 + linux/arm64;
- OCI Helm chart `oci://ghcr.io/dgeliko/charts/k8s-alert`, version 0.1.6;
- GitHub Release с обоими скомпилированными Linux-бинарниками,
  Helm `.tgz` и SHA256SUMS; исходники доступны в Git и release source archives.

Бинарники храним в Releases, не в истории исходников. Публикация выполняется
через GITHUB_TOKEN с packages:write и contents:write; постоянный PAT не нужен.

Сценарии проверки в dev-кластере: [docs/dev-smoke.md](docs/dev-smoke.md).


При каждом push в main и pull request CI дополнительно проверяет Docker-сборку
и запуск `--version`. Linux-бинарники и Helm-чарт собираются при публикации релиза. Никакие локальные Docker-сокеты не используются.
