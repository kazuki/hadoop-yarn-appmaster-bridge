# YARN ApplicationMaster Bridge Application

Javaで記述する必要のあるYARNアプリケーションを，
他の言語から記述可能にするブリッジです．
Java製のAppMaster橋渡しプログラムから，
任意の言語で記述したAppMasterを起動し，
その間をJSON-RPC2.0ライクなプロトコルで結ぶことで実現しています．

サンプルとしてPythonで記述したYARNアプリケーションを同梱しています．
(サンプルはPython3.4以降 + [python-asyncio-rpc](https://github.com/kazuki/python-asyncio-rpc/)が必要です)

## アプリケーションのSubmit方法

引数にはYARNのLocalResourceとしてAppMaster実行コンテナに配置するファイルを指定します(HDFS上のパスである必要があります)

```
$ mvn package
$ sudo cp ./target/yarn-app-bridge-1.0.0.jar ${HADOOP_HOME}/share/hadoop/common/
$ hadoop org.oikw.yarn_bridge.Client [本当のAppMasterを実行するシェルスクリプトのHDFS上のパス] [その他AppMasterを実行するのに必要なファイルのHDFS上のパス] ...
```

必要に応じて以下のオプションを指定することが出来ます

* --no-wait: ApplicationMasterの終了を待つこと無くクライアントを終了します
* --name: Application名として登録する名前を指定します (デフォルト: Application)
* --queue: 利用するキュー名を指定します (デフォルト: default)
* --cpu: ApplicationMaster用に確保するVCPU数を指定します (デフォルト: 1)
* --memory: ApplicationMaster用に確保するメモリサイズを指定します (デフォルト: 1024[MB])
* --: これ以降の引数はそのままAppMasterに渡すことを指示します

ApplicationMaster用コンテナ上では，AppMasterBridge(Java)プログラムが起動し，
Clientの最初の引数で指定されたシェルスクリプトを実行します(/bin/bash 経由)．
そのシェルスクリプトには引数として，AppMasterBridgeとの通信用TCPポート番号および，
Clientに渡した二番目以降の引数が全て渡ります．

また，引数に指定するHDFS上のパスの先頭に"@"を付与すると，
そのファイルをAppMaster以外の全てのコンテナのLocalResourceとして利用することを指示します．

## AppMasterBridgeとAppMaster間のRPC

基本的には[JSON-RPC 2.0](http://www.jsonrpc.org/)に準拠していますが，
サーバサイドからのプッシュ通知を追加しています．

### AppMasterBridge -> AppMaster

(JSON-RPCのNotificationメッセージ)

```
{
  "method": "onContainersAllocated",
  "params": {
    "containers": [{
      "container_id": str,
      "cpu": int,
      "memory": int,
      "priority": int,
      "node_id": str
    }, ...]
  }
}
```

```
{
  "method": "onContainersCompleted",
  "params": {
    "statuses": [{
      "container_id": str,
      "diagnostic": str,
      "exit_status": int
    }, ...]
  }
}
```

```
{
  "method": "onShutdownRequest"
}
```

```
{
  "method": "onError"
}
```

### AppMaster -> AppMasterBridge

(JSON-RPCのRequest/Responseメッセージ)

```
Req:
{
  "method": "addContainerRequest",
  "params": {
    "memory": int,
    "cpu": int
    "priority": int,
  }
}

Res:
{
  "result": null
}
```

```
Req:
{
  "method": "getAvailableResources"
}

Res:
{
  "result": {
    "memory": int,
    "cpu": int
  }
}
```

```
Req:
{
  "method": "getClusterNodeCount"
}

Res:
{
  "result": int
}
```

```
Req:
{
  "method": "releaseAssignedContainer",
  "params": {
    "container_id": str,
  }
}

Res:
{
  "result": null
}
```

```
Req:
{
  "method": "startContainer",
  "params": {
    "container_id": str,
    "command": str,
  }
}

Res:
{
  "result": null
}
```
