## ECE419 Data Storage Application

### Environment Setup

1. Setup password-less SSH login. That is, `ssh 127.0.0.1` needs to work.
2. Add an environment variable `ECE419_SERVER_PATH` storing the path to this
   folder. For example, add the following to `~/.bashrc`:
   ```bash
   export ECE419_SERVER_PATH="/path/to/this/folder"
   ```
3. Install and start Apache ZooKeeper, with port set to `2181`.

### Building and Running

1. Run `ant`.
2. Java jar files will be build under this directory. Make sure to run them with
   the current directory set to this directory.
