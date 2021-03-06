package utils;

import redis.clients.jedis.HostAndPort;
import redis.clients.jedis.Jedis;

import java.io.IOException;
import java.io.ObjectStreamException;
import java.io.Serializable;

public class JedisImplSer implements Serializable {
    private static final long serialVersionUID = -57L;
    private transient Jedis jedis;
    private HostAndPort hostAndPort;
    public JedisImplSer(HostAndPort hostAndPort) {
        this.jedis = new Jedis(hostAndPort);
        this.hostAndPort = hostAndPort;
    }

    private void writeObject(java.io.ObjectOutputStream out) throws IOException {
        out.defaultWriteObject();
        out.writeObject(hostAndPort);
    }

    private void readObject(java.io.ObjectInputStream in) throws IOException, ClassNotFoundException{
        in.defaultReadObject();
        setJedis(new Jedis(hostAndPort));
    }

    private void readObjectNoData() throws ObjectStreamException {

    }

    public Jedis getJedis() {
        return jedis;
    }

    private void setJedis(Jedis jedis) {
        this.jedis = jedis;
    }
}
