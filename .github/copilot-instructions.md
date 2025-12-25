# Copilot Instructions — 纽约出租车 项目

目的：让 AI 编码代理（Copilot / Assistants）快速上手本仓库，包含可执行命令、重要约定、以及从代码中可发现的具体示例。

- **快速启动**
  - 运行环境：使用 Python 3.8+ 虚拟环境。推荐命令（Windows PowerShell）：

    ```powershell
    python -m venv .venv
    .\.venv\Scripts\Activate.ps1
    python -m pip install --upgrade pip
    pip install pandas pyarrow
    ```

  - 运行脚本：仓库根目录下直接运行 `check_data.py`：
    - `python check_data.py`

- **代码库一览（大局观）**
  - 目前仓库非常精简：主要为一个用于快速检查 parquet 数据的脚本 `check_data.py`。
  - 该脚本演示了项目关注点：读取大型出租车数据文件（parquet），打印前几行与列名用于快速核查。
    - 参见示例：[check_data.py](check_data.py#L1-L20)

- **可发现的约定与模式**
  - 使用 `pandas.read_parquet(..., engine='pyarrow')` 来读取 parquet 文件（脚本内明确指定引擎）。这是本仓库读取 parquet 的约定；如果改动引擎，需同时保证依赖已安装。
  - 文件路径由顶部变量 `file_path` 设置，代理在修改或自动化时应优先保持该变量位置不变以便快速替换样例文件名。

- **常见任务与示例（可直接执行）**
  - 替换样例文件名：把 `file_path` 改为目标文件（脚本顶部）。示例位置：[check_data.py](check_data.py#L3-L6)
  - 当文件很大时，脚本目前只做 `df.head()` 打印，避免一次性输出全表。若需进一步处理，应先做列选择或样本抽取。

- **调试与限制说明（代理须知）**
  - parquet 文件可能很大，内存受限时会 OOM。优先建议：用小样本文件测试（本仓库示例即采用打印前几行的方式），或采用按需列读取 / Dask 等外部工具 — 但仅在用户明确要求时引入新依赖。
  - 在 Windows PowerShell 下，虚拟环境激活命令与 Linux/WSL 不同；生成或运行脚本时请用上文示例的 PowerShell 命令。

- **编辑/提交风格**
  - 保持变更小而明确：对 `check_data.py` 的改动若只是更换演示文件名或打印内容，应避免重构成复杂模块，除非用户要求构建库或增加单元测试。

- **何时询问用户**
  - 如果需要安装额外依赖（如 Dask、fastparquet），先询问并说明内存/性能权衡。
  - 在对脚本做重大结构性改动（如把脚本拆成包、添加 CLI、引入配置文件）前，先与用户确认需求与目标。

若本文件有遗漏或需要把脚本拓展为小型数据处理库，请告诉我想要的方向（例如：添加 CLI、增加采样/过滤、或支持更大数据流式读取），我会据此更新或扩展说明。
