# mongo sync

To clone in real time using mongo stream a cluter into another cluster with the option to chnage the databanse name and teh collection name.

## How to run it

- Create a file similar to options.js at the root folder and set the right configuration 
- Set an environment variable to point to this file
- run the code

here is a bash script example 
```bash
export SOAJS_MONGO_SYNC_OPTIONS=options.js

node main.js
```

### License
*Copyright SOAJS All Rights Reserved.*

Use of this source code is governed by an Apache license that can be found in the LICENSE file at the root of this repository.
