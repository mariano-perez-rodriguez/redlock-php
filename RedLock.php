<?php

/**
 * RedLock (http://redis.io/topics/distlock) implementation
 *
 */
class RedLock {
  /**
   * Lua unlocking script
   *
   * @var string
   */
  protected static $unlockLUA = 'if redis.call("GET", KEYS[1]) == ARGV[1] then return redis.call("DEL", KEYS[1]) else return 0 end';

  /**
   * SHA1 hex string of the Lua unlocking script, may be null to recalculate it each time
   *
   * @var string|null
   */
  protected static $unlockSHA = '013976d92eab7585a652ddbf0f4ae3746f38b191';

  /**
   * Lua extension script
   *
   * @var string
   */
  protected static $extendLUA = 'if redis.call("GET", KEYS[1]) == ARGV[1] then return redis.call("PEXPIRE", KEYS[1], ARGV[2]) else return 0 end';

  /**
   * SHA1 hex string of the Lua extension script, may be null to recalculate it each time
   *
   * @var string|null
   */
  protected static $extendSHA = 'c50343e01dd9310b37fcced0dccd5ac1eecff492';

  /**
   * Determine whether the given parameter is an associative array
   *
   * @param mixed $x  Parameter to use
   * @return bool
   */
  protected static function is_assoc($x) {
    return is_array($x) && count($x) === count(array_filter(array_keys($x), 'is_string'));
  }

  /**
   * Determine whether the given parameter is a sequential array
   *
   * @param mixed $x  Parameter to use
   * @return bool
   */
  protected static function is_sequential($x) {
    return is_array($x) && count($x) === count(array_filter(array_keys($x), 'is_integer'));
  }

  /**
   * Distill a configuration array from the given mixed parameter
   *
   * The given parameter may be:
   *  - an associative array with keys: 'host', 'port', 'db', 'timeout',
   *    'persistentId', 'auth', 'serializer', and 'prefix',
   *  - a positional array, with values for each of the above given keys in turn,
   *  - an stdClass object, with fields named as the above mentioned keys,
   *  - a \Redis instance.
   *
   * @param mixed $cfg  The configuration parameter to extract configuration data from
   * @return array  The configuration array to use
   */
  protected static function distillCfg($cfg) {
    static $defaults = [
      'host'         => '127.0.0.1',
      'port'         => 6379,
      'db'           => null,
      'timeout'      => 0,
      'persistentId' => null,
      'auth'         => null,
      'serializer'   => \Redis::SERIALIZER_NONE,
      'prefix'       => null,
      //
      'instance'     => null,
    ];

    $result = null;

    switch (true) {
      case static::is_assoc($cfg):
        $result = $cfg + $defaults;
        $result['instance'] = null;
        break;
      case static::is_sequential($cfg):
        $result = array_combine(array_keys($defaults), array_slice($cfg + array_values($defaults), 0, count($defaults)));
        $result['instance'] = null;
        break;
      case is_a($cfg, '\\stdClass'):
        $result = ((array) $cfg) + $defaults;
        $result['instance'] = null;
        break;
      case is_a($cfg, '\\Redis'):
        $result = (!$cfg->isConnected() ? [] : [
          'host'         => $cfg->getHost(),
          'port'         => $cfg->getPort(),
          'db'           => $cfg->getDbNum(),
          'timeout'      => $cfg->getTimeout(),
          'persistentId' => $cfg->getPersistentID(),
          'auth'         => $cfg->getAuth(),
          'serializer'   => $cfg->getOption(\Redis::OPT_SERIALIZER),
          'prefix'       => $cfg->getOption(\Redis::OPT_PREFIX),
        ]) + $defaults;
        $result['instance'] = $cfg;
        break;
      default:
        return false;
    }

    return array_intersect_key($result, $defaults);
  }

  /**
   * Return the number of microseconds since the epoch
   *
   * @return int  Number of microseconds since the epoch
   */
  protected static function usecs() {
    $m = microtime();
    return (int) (substr($m, 11) . substr($m, 2, 6));
  }

  /**
   * Return the number of milliseconds since the epoch
   *
   * @return int  Number of milliseconds since the epoch
   */
  protected static function msecs() {
    return (int) (static::usecs() / 1000);
  }

  /**
   * Return a new 160-bit hex token generated randomly
   *
   * @return string  A 160-bit random hex token
   */
  protected static function newToken() {
    static $token = null;
    if (null === $token) {
      $token = file_get_contents('/dev/urandom', false, null, 0, 20);
    }

    return bin2hex($token = sha1($token . pack('P', static::usecs()), true));
  }

  /**
   * Perform a locking operation
   *
   * @param \Redis $redis  Redis instance to use
   * @param string $resource  Resource name to lock
   * @param string $token  Random token to use
   * @param int $ttl  Milliseconds to use for lock time
   * @return bool  Whether the command was successful or not
   * @throws \RedisException  If a connection error occurs
   */
  protected static function doLock(\Redis $redis, $resource, $token, $ttl) {
    return $redis->set($resource, $token, ['NX', 'PX' => $ttl]);
  }

  /**
   * Perform an unlocking operation
   *
   * @param \Redis $redis  Redis instance to use
   * @param string $resource  Resource name to unlock
   * @param string $token  Random token to use
   * @return bool  Whether the command was successful or not
   * @throws \RedisException  If a connection error occurs
   */
  protected static function doUnlock(\Redis $redis, $resource, $token) {
    return $redis->evalSha(static::$unlockSHA, [$resource, $token], 1);
  }

  /**
   * Perform an extension operation
   *
   * @param \Redis $redis  Redis instance to use
   * @param string $resource  Resource name to extend the lock on
   * @param string $token  Random token to use
   * @param int $ttl  Milliseconds to use for lock extension time
   * @return bool  Whether the command was successful or not
   * @throws \RedisException  If a connection error occurs
   */
  protected static function doExtend(\Redis $redis, $resource, $token, $ttl) {
    return $redis->evalSha(static::$extendSHA, [$resource, $token, $ttl], 1);
  }

  /**
   * Servers configuration and instances array
   *
   * @var array
   */
  protected $servers = [];

  /**
   * Server quorum count
   *
   * @var int
   */
  protected $quorum = 0;

  /**
   * Maximum retry delay in milliseconds
   *
   * @var int
   */
  protected $retryDelay = 200;

  /**
   * Number of retries to attempt
   *
   * @var int
   */
  protected $retryCount = 3;

  /**
   * Clock drift multiplicative factor
   *
   * @var float
   */
  protected $clockDriftFactor = 0.01;

  /**
   * Try to initialize all the servers in the server list
   *
   * This function will leave the servers with a valid instance alone, but it will
   * try to reconnect every server without a valid instance.
   *
   * @return int  The current quorum count
   */
  protected function init() {
    // calculate Lua unlocking script SHA1 if not already calculated
    if (null === static::$unlockSHA) {
      static::$unlockSHA = strtolower(sha1(static::$unlockLUA));
    }
    // calculate Lua extension script SHA1 if not already calculated
    if (null === static::$extendSHA) {
      static::$extendSHA = strtolower(sha1(static::$extendLUA));
    }

    $n = 0;
    foreach ($this->servers as &$server) {
      if (null !== $server['instance']) {
        // if the instance was already initialized, just add it to the quorum count
        $n++;
      } else {
        try {
          // create a new instance and try to connect using persistent connections if appropriate
          $instance = new \Redis();
          if (null === $server['persistentId']) {
            $instance->connect($server['host'], $server['port'], $server['timeout']);
          } else {
            $instance->pconnect($server['host'], $server['port'], $server['timeout'], $server['persistentId']);
          }
          // apply connection settings
          if ($instance->isConnected()
          && ((null === $server['auth']) || $instance->auth($server['auth']))
          && ((null === $server['db'  ]) || $instance->select($server['db']))
          // apply client settings
          && ($server['prefix']     || $instance->setOption(\Redis::OPT_PREFIX,     $server['prefix'    ]))
          && ($server['serializer'] || $instance->setOption(\Redis::OPT_SERIALIZER, $server['serializer']))
          // load scripts
          && (strtolower($instance->script('load', static::$unlockLUA)) === static::$unlockSHA)
          && (strtolower($instance->script('load', static::$extendLUA)) === static::$extendSHA)
          ) {
            // if everything went well, add the instance to the server list and increase the quorum count
            $server['instance'] = $instance;
            $n++;
          }
        } catch (\RedisException $e) {
          // do nothing
        }
      }
    }

    // return the quorum count
    return $n;
  }

  /**
   * Apply the given callback to each instance in turn
   *
   * This function will call the given callback passing the server instance in its
   * first argument, and the additional arguments given after that, it will count the
   * number of successful such callings and return that. If the callback throws a
   * \RedisException, the instance is removed.
   *
   * @param callable $cb  The callback to apply
   * @param variadic $args  Additional arguments to pass
   * @return int  The number of successful calls performed
   */
  protected function forAllInstaces(callable $cb, ...$args) {
    $n = 0;
    foreach ($this->servers as &$server) {
      try {
        if (null !== $server['instance'] && $cb($server['instance'], ...$args)) {
          $n++;
        }
      } catch (\RedisException $e) {
        // remove the instance on connection error
        $server['instance'] = null;
      }
    }
    return $n;
  }

  /**
   * Initialize a RedLock instance
   *
   * @param array $servers  Server configurations to use
   * @param int $retryDelay  Delay to apply during retries
   * @param int $retryCount  Number of retries to apply
   * @param float $clockDriftFactor  Clock drift multiplicative factor to use
   * @param bool $init  Whether to initialize the server instances right away or not
   * @throws \Exception  If empty server list, or invalid server entry, or malformed retry delay or count or clock drift factor
   */
  public function __construct(array $servers, $retryDelay = 200, $retryCount = 3, $clockDriftFactor = 0.01, $init = false) {
    if ([] === $servers) {
      throw new \Exception("Empty server list");
    }
    $i = 0;
    foreach ($servers as $server) {
      if (false === ($server = static::distillCfg($server))) {
        throw new \Exception("Invalid server entry at index {$i}");
      }
      $this->servers[] = $server;
      $i++;
    }
    $this->quorum = min($i, ($i / 2) + 1);

    if (($retryDelay = ((int) $retryDelay) ?: 200) < 0) {
      throw new \Exception("Retry delay must be non-negative");
    }
    if (($retryCount = ((int) $retryCount) ?: 3) < 0) {
      throw new \Exception("Retry count must be non-negative");
    }
    if (($clockDriftFactor = ((float) $clockDriftFactor) ?: 0.01) < 0) {
      throw new \Exception("Clock drift factor must be non-negative");
    }

    $this->retryDelay       = $retryDelay;
    $this->retryCount       = $retryCount;
    $this->clockDriftFactor = $clockDriftFactor;

    if ($init) {
      $this->init();
    }
  }

  /**
   * Clone the current object by cloning every instance in the server list
   *
   */
  public function __clone() {
    foreach ($this->servers as &$server) {
      if (null !== $server['instance']) {
        $server['instance'] = clone $server['instance'];
      }
    }
  }

  /**
   * Try to acquire a lock on the given resource
   *
   * @param string $resource  Resource to acquire a lock for
   * @param int $ttl  TTL to associate to the lock
   * @param string $token  Token to use for the lock (if none given, create a new one)
   * @return array|false  Returns an array with 'resource', 'token', and 'validity' keys if successful, false otherwise
   * @throws \Exception  In case no quorum achieved or malformed ttl
   */
  public function lock($resource, $ttl = 500, $token = null) {
    // sanitize ttl
    if (($ttl = ((int) $ttl) ?: 500) < 0) {
      throw new \Exception("TTL must be non-negative");
    }
    // create token if none given
    if (null === $token) {
      $token = static::newToken();
    }

    // try to initialize and get quorum
    if ($this->init() < $this->quorum) {
      throw new \Exception("No quorum");
    }

    // set everything up for main cycle
    $lock  = ['resource' => $resource, 'token' => $token];
    $retry = $this->retryCount;
    do {
      // save starting time and apply callback to all instances
      $startTime = static::msecs();
      $n = $this->forAllInstaces([__CLASS__, 'doLock'], $resource, $token, $ttl);

      # Add 2 milliseconds to the drift to account for Redis expires
      # precision, which is 1 millisecond, plus 1 millisecond min drift
      # for small TTLs.
      $drift = ($ttl * $this->clockDriftFactor) + 2;

      $validityTime = $ttl - (static::msecs() - $startTime) - $drift;

      // if quorum achieved and time remaining, return
      if ($this->quorum <= $n && 0 < $validityTime) {
        return $lock + ['validity' => $validityTime];
      }

      // otherwise, clean up
      $this->unlock($lock);

      // Wait a random delay before to retry
      usleep(mt_rand(floor($this->retryDelay / 2), $this->retryDelay) * 1000);
    } while (--$retry > 0);

    // no lock acquired
    return false;
  }

  /**
   * Free a previously acquired lock
   *
   * @param array $lock  A lock array with keys 'resource' and 'token'
   * @return boolean  Whether quorum was reached regarding the freeing of the lock
   * @throws \Exception  In case no quorum achieved or malformed lock
   */
  public function unlock(array $lock) {
    // sanitize lock
    if (!array_key_exists('resource', $lock)) {
      throw new \Exception("Missing 'resource'");
    }
    if (!array_key_exists('token', $lock)) {
      throw new \Exception("Missing 'token'");
    }

    // try to initialize and get quorum
    if ($this->init() < $this->quorum) {
      throw new \Exception("No quorum");
    }

    // apply callback to all instances and return whether quorum was reached
    return $this->quorum <= $this->forAllInstaces([__CLASS__, 'doUnlock'], $lock['resource'], $lock['token']);
  }

  /**
   * Extend the TTL on a previously acquired lock
   *
   * @param array $lock  A lock array with keys 'resource' and 'token'
   * @param int $ttl  New TTL to use
   * @return boolean  Whether quorum was reached regarding the extension of the lock
   * @throws \Exception  In case no quorum achieved or malformed lock or ttl
   */
  public function extend(array $lock, $ttl = 500) {
    // sanitize lock
    if (!array_key_exists('resource', $lock)) {
      throw new \Exception("Missing 'resource'");
    }
    if (!array_key_exists('token', $lock)) {
      throw new \Exception("Missing 'token'");
    }
    // sanitize ttl
    if (($ttl = ((int) $ttl) ?: 500) < 0) {
      throw new \Exception("TTL must be non-negative");
    }

    // try to initialize and get quorum
    if ($this->init() < $this->quorum) {
      throw new \Exception("No quorum");
    }

    // apply callback to all instances and return whether quorum was reached
    return $this->quorum <= $this->forAllInstaces([__CLASS__, 'doExtend'], $lock['resource'], $lock['token'], $ttl);
  }

  /**
   * Run the given callback locking the given resource
   *
   * The callback given wil be passed the lock obtained as its first argument.
   *
   * @param string $resource  Resource to lock on
   * @param callable $cb  Callback to apply (the lock will be passed as first parameter)
   * @param int $ttl  TTL to associate to the lock
   * @return mixed  Whatever the callback returns
   * @throws \Exception  In case of malformed TTL
   */
  public function with($resource, callable $cb, $ttl = 500) {
    // sanitize ttl
    if (($ttl = ((int) $ttl) ?: 500) < 0) {
      throw new \Exception("TTL must be non-negative");
    }

    // declare the lock and the result
    $lock = false;
    $result = null;
    try {
      // try to acquire the lock on the resource
      if (false !== ($lock = $this->lock($resource, $ttl))) {
        $result = $cb($lock);
      }
    } finally {
      // one way or another, try to unlock when done if needed
      if (false !== $lock) {
        $this->unlock($lock);
      }
    }

    return $result;
  }
}
