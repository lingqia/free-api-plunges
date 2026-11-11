# Free API 插件仓库

[free-api](https://github.com/lingqia/free-api) 网关的站点插件集合。每个插件一个目录、**一个独立 Go module**，互不影响；网关仓库不包含任何插件代码。

## 目录规范

```
plugins/
  <插件名>/
    go.mod        # 本插件独立 module（module free-api-plunges/<插件名>）
    main.go       # 插件入口（NDJSON-RPC，协议见网关仓库 docs/plugin-requirements.md）
    README.md     # 站点说明：端点、凭证、限流、构建方法
    bin/          # 构建产物（不入库）
```

新增一个插件 = 新增一个 `plugins/<名称>/` 目录，不需要动仓库根部的任何文件。

## 构建单个插件

```bash
cd plugins/<插件名>
go build -o bin/<插件名> .
./bin/<插件名> --selfcheck    # 自检：打印模型清单，不连上游
```

## 接入网关

把插件二进制拷到网关目录 `plugins/<插件名>/bin/`，在网关 `configs/gateway.yaml` 的 `plugins:` 里声明（`command`、`tools`、`models`、`auth` 等），字段说明见网关仓库 `docs/plugin-requirements.md`。
