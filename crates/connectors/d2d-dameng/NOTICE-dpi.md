# NOTICE — DPI FFI declarations

`src/dpi/ffi.rs` 中的函数声明与常量由达梦 DM8 SDK 的头文件
（`DPI.h` / `DPItypes.h` / `DPIext.h`）派生，版权归**武汉达梦数据库股份有限公司**所有，
随其 DM8 客户端 SDK 发布。

本仓库**不包含**上述头文件，也**不分发** `libdmdpi.so` / `dmdpi.dll`：
- 头文件只在开发阶段用于核对签名；
- 动态库由目标机器上已授权的达梦客户端安装提供，运行期通过
  `source.properties.dpi.library` / `sink.properties.dpi.library` 或
  `LD_LIBRARY_PATH` 加载。

分发本仓库时请保留本说明；将本连接器与达梦客户端一起分发前，请自行确认
达梦 SDK 的许可条款。
