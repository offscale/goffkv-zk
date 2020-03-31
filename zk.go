package goffkv_zk

import (
    "time"
    "bytes"
    goffkv "github.com/offscale/goffkv"
    zkapi "github.com/samuel/go-zookeeper/zk"
)

const (
    ttl = time.Second * 10
)

var (
    defaultAcl = []zkapi.ACL{
        zkapi.ACL{
            Scheme: "world",
            ID: "anyone",
            Perms: zkapi.PermAll,
        },
    }
)

type zkClient struct {
    conn *zkapi.Conn
    prefixSegments []string
}

func (c *zkClient) assemblePath(segments []string) string {
    var result bytes.Buffer

    for _, segment := range c.prefixSegments {
        result.WriteByte('/')
        result.WriteString(segment)
    }
    for _, segment := range segments {
        result.WriteByte('/')
        result.WriteString(segment)
    }

    return result.String()
}

func createEachPrefix(conn *zkapi.Conn, segments []string) error {
    var prefix bytes.Buffer

    for _, segment := range segments {
        prefix.WriteByte('/')
        prefix.WriteString(segment)

        _, err := conn.Create(prefix.String(), nil, 0, defaultAcl)
        if err != nil && err != zkapi.ErrNodeExists {
            return err
        }
    }

    return nil
}

func convertError(err error) error {
    switch err {
    case zkapi.ErrNodeExists:
        return goffkv.OpErrEntryExists
    case zkapi.ErrNoNode:
        return goffkv.OpErrNoEntry
    case zkapi.ErrNoChildrenForEphemerals:
        return goffkv.OpErrEphem
    default:
        return err
    }
}

func New(address string, prefix string) (goffkv.Client, error) {
    prefixSegments, err := goffkv.DisassemblePath(prefix)
    if err != nil {
        return nil, err
    }

    conn, _, err := zkapi.Connect([]string{address}, ttl)
    if err != nil {
        return nil, err
    }

    err = createEachPrefix(conn, prefixSegments)
    if err != nil {
        conn.Close()
        return nil, err
    }

    return &zkClient{
        conn: conn,
        prefixSegments: prefixSegments,
    }, nil
}

func (c *zkClient) Create(key string, value []byte, lease bool) (goffkv.Version, error) {
    segments, err := goffkv.DisassembleKey(key)
    if err != nil {
        return 0, err
    }

    var flags int32
    if lease {
        flags = zkapi.FlagEphemeral
    }

    _, err = c.conn.Create(c.assemblePath(segments), value, flags, defaultAcl)
    if err != nil {
        return 0, convertError(err)
    }

    return 1, nil
}

func (c *zkClient) Set(key string, value []byte) (goffkv.Version, error) {
    segments, err := goffkv.DisassembleKey(key)
    if err != nil {
        return 0, err
    }

    _, err = c.conn.Create(c.assemblePath(segments), value, 0, defaultAcl)
    if err == nil {
        return 1, nil
    }

    if err != zkapi.ErrNodeExists {
        return 0, convertError(err)
    }

    stat, err := c.conn.Set(c.assemblePath(segments), value, -1)
    if err == nil {
        return uint64(stat.Version) + 1, nil
    }

    if err == zkapi.ErrNoNode {
        return uint64(1) << 62, nil
    }
    return 0, convertError(err)
}

func (c *zkClient) Cas(key string, value []byte, ver goffkv.Version) (goffkv.Version, error) {
    if ver == 0 {
        resultVer, err := c.Create(key, value, false)
        if err == nil {
            return resultVer, nil
        }
        if err == goffkv.OpErrEntryExists {
            return 0, nil
        }
        return 0, err
    }

    segments, err := goffkv.DisassembleKey(key)
    if err != nil {
        return 0, err
    }

    stat, err := c.conn.Set(c.assemblePath(segments), value, int32(ver - 1))
    switch err {
    case nil:
        return uint64(stat.Version) + 1, nil
    case zkapi.ErrBadVersion:
        return 0, nil
    default:
        return 0, convertError(err)
    }
}

func (c *zkClient) makeEraseQuery(ops []interface{}, segments []string) ([]interface{}, error) {
    path := c.assemblePath(segments)
    children, _, err := c.conn.Children(path)
    if err != nil {
        return ops, err
    }

    for _, child := range children {
        ops, err = c.makeEraseQuery(ops, append(segments, child))
        if err != nil && err != zkapi.ErrNoNode {
            return ops, err
        }
    }
    ops = append(ops, &zkapi.DeleteRequest{
        Path: path,
        Version: -1,
    })
    return ops, nil
}

func (c *zkClient) Erase(key string, ver goffkv.Version) error {
    segments, err := goffkv.DisassembleKey(key)
    if err != nil {
        return err
    }

outermost:
    for {
        ops := []interface{}{
            &zkapi.CheckVersionRequest{
                Path: c.assemblePath(segments),
                Version: int32(ver) - 1,
            },
        }

        ops, err = c.makeEraseQuery(ops, segments)
        if err != nil {
            return convertError(err)
        }

        data, err := c.conn.Multi(ops...)
        switch err {
        case nil:
            return nil
        case zkapi.ErrBadVersion:
            return nil
        case zkapi.ErrNotEmpty:
            continue outermost
        case zkapi.ErrNoNode:
            if data[0].Error != nil {
                return goffkv.OpErrNoEntry
            } else {
                continue outermost
            }
        default:
            return convertError(err)
        }
    }
}

func (c *zkClient) Exists(key string, watch bool) (goffkv.Version, goffkv.Watch, error) {
    segments, err := goffkv.DisassembleKey(key)
    if err != nil {
        return 0, nil, err
    }

    var (
        exists bool
        stat *zkapi.Stat
        resultWatch goffkv.Watch
    )

    if watch {
        var ech <-chan zkapi.Event
        exists, stat, ech, err = c.conn.ExistsW(c.assemblePath(segments))
        if err != nil {
            return 0, nil, convertError(err)
        }
        resultWatch = func() {
            <-ech
        }

    } else {
        exists, stat, err = c.conn.Exists(c.assemblePath(segments))
        if err != nil {
            return 0, nil, convertError(err)
        }
    }

    var resultVer uint64
    if exists {
        resultVer = uint64(stat.Version) + 1
    }
    return resultVer, resultWatch, nil
}

func (c *zkClient) Get(key string, watch bool) (goffkv.Version, []byte, goffkv.Watch, error) {
    segments, err := goffkv.DisassembleKey(key)
    if err != nil {
        return 0, nil, nil, err
    }

    var (
        result []byte
        stat *zkapi.Stat
        resultWatch goffkv.Watch
    )

    if watch {
        var ech <-chan zkapi.Event
        result, stat, ech, err = c.conn.GetW(c.assemblePath(segments))
        if err != nil {
            return 0, nil, nil, convertError(err)
        }
        resultWatch = func() {
            <-ech
        }

    } else {
        result, stat, err = c.conn.Get(c.assemblePath(segments))
        if err != nil {
            return 0, nil, nil, convertError(err)
        }
    }

    return uint64(stat.Version) + 1, result, resultWatch, nil
}

func (c *zkClient) Children(key string, watch bool) ([]string, goffkv.Watch, error) {
    segments, err := goffkv.DisassembleKey(key)
    if err != nil {
        return nil, nil, err
    }

    var (
        rawChildren []string
        resultWatch goffkv.Watch
    )

    if watch {
        var ech <-chan zkapi.Event
        rawChildren, _, ech, err = c.conn.ChildrenW(c.assemblePath(segments))
        if err != nil {
            return nil, nil, convertError(err)
        }
        resultWatch = func() {
            <-ech
        }

    } else {
        rawChildren, _, err = c.conn.Children(c.assemblePath(segments))
        if err != nil {
            return nil, nil, convertError(err)
        }
    }

    result := []string{}
    for _, rawChild := range rawChildren {
        result = append(result, key + "/" + rawChild)
    }
    return result, resultWatch, nil
}

type resultKind int
const (
    rkCreate resultKind = iota
    rkSet
    rkAux
)

func toUserOpIndex(boundaries []int, op int) int {
    for i, x := range boundaries {
        if x >= op {
            return i
        }
    }
    return -1
}

func (c *zkClient) Commit(txn goffkv.Txn) ([]goffkv.TxnOpResult, error) {
outermost:
    for {
        boundaries := []int{}
        ops := []interface{}{}
        rks := []resultKind{}

        for _, check := range txn.Checks {
            segments, err := goffkv.DisassembleKey(check.Key)
            if err != nil {
                return nil, err
            }

            ops = append(ops, &zkapi.CheckVersionRequest{
                Path: c.assemblePath(segments),
                Version: int32(check.Ver) - 1,
            })

            boundaries = append(boundaries, len(ops) - 1)
            rks = append(rks, rkAux)
        }

        for _, op := range txn.Ops {
            segments, err := goffkv.DisassembleKey(op.Key)
            if err != nil {
                return nil, err
            }

            switch op.What {
            case goffkv.Create:
                var flags int32
                if op.Lease {
                    flags = zkapi.FlagEphemeral
                }
                ops = append(ops, &zkapi.CreateRequest{
                    Path: c.assemblePath(segments),
                    Data: op.Value,
                    Acl: defaultAcl,
                    Flags: flags,
                })
                rks = append(rks, rkCreate)

            case goffkv.Set:
                ops = append(ops, &zkapi.SetDataRequest{
                    Path: c.assemblePath(segments),
                    Data: op.Value,
                    Version: -1,
                })
                rks = append(rks, rkSet)

            case goffkv.Erase:
                oldNops := len(ops)
                ops, err = c.makeEraseQuery(ops, segments)
                if err != nil {
                    if err != zkapi.ErrNoNode {
                        return nil, convertError(err)
                    }
                    ops = append(ops, &zkapi.DeleteRequest{
                        Path: c.assemblePath(segments),
                        Version: -1,
                    })
                }
                for i := oldNops; i < len(ops); i++ {
                    rks = append(rks, rkAux)
                }
            }

            boundaries = append(boundaries, len(ops) - 1)
        }

        data, err := c.conn.Multi(ops...)
        // Note: err is checked later.

        result := []goffkv.TxnOpResult{}
        for i, datum := range data {
            if datum.Error != nil {
                userIndex := toUserOpIndex(boundaries, i)
                if userIndex < 0 {
                    panic("txn failed on non-existing op")
                }
                if boundaries[userIndex] != i {
                    continue outermost
                }
                return nil, goffkv.TxnError{userIndex}
            }

            switch rks[i] {
            case rkCreate:
                result = append(result, goffkv.TxnOpResult{
                    goffkv.Create,
                    1,
                })
            case rkSet:
                result = append(result, goffkv.TxnOpResult{
                    goffkv.Set,
                    uint64(datum.Stat.Version) + 1,
                })
            }
        }

        if err != nil {
            return nil, convertError(err)
        }
        return result, nil
    }
}

func (c *zkClient) Close() {
    c.conn.Close()
}

func init() {
    goffkv.RegisterClient("zk", New)
}
