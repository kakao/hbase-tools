# hbase-tools
Collection of command-line tools for HBase. 

# Modules
1. hbase-common
   - Shared Library
   - Adaptor for API Compatibility
2. hbase-manager
   - Toolkit for
     - Region Assignment Management
     - Major Compaction
     - Merging and Splitting Regions
3. hbase-table-stat
   - Cluster Performance Monitor
4. hbase-snapshot
   - Table Snapshot Manager

# Usage
1. Download jars from [releases](../../releases) page
    - Or build it by using [package.sh](package.sh) on your PC
        - JDK7 and Maven 3.2 are required
2. Run downloaded jar with JRE
   ```
   java -jar <module>-<version of HBase>-<version of release>.jar [options] <args...>
   ```
   
# Resources
  - [Introduction and Use Cases(Korean)](../../releases/download/v1.1.1/hbase-tools-korean.pdf)

# License
This software is licensed under the [Apache 2 license](LICENSE.txt), quoted below.

Copyright 2015 Kakao Corp. <http://www.kakaocorp.com>

Licensed under the Apache License, Version 2.0 (the "License"); you may not use this project except in compliance with the License. You may obtain a copy of the License at http://www.apache.org/licenses/LICENSE-2.0.

Unless required by applicable law or agreed to in writing, software distributed under the License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the License for the specific language governing permissions and limitations under the License.
