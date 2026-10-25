package baseclient

import (
    "encoding/base64"
    "encoding/json"
    "fmt"
    "sort"
    "strings"
)

type AuthFetcher func(forceRefresh bool) (*string, error)

type LocalSyncState struct {
    nextQueryID      uint64
    nextSubscriberID int64
    tokenToQueryID   map[string]uint64
    querySubscribers map[uint64]map[int64]struct{}
    subscriberQuery  map[int64]uint64
    queryValues      map[uint64]any
    querySetVersion  uint64
    identityVersion  uint64
    authFetcher      AuthFetcher
    authToken        *string
    observedTS       uint64
}

func NewLocalSyncState() *LocalSyncState {
    return &LocalSyncState{
        tokenToQueryID:   map[string]uint64{},
        querySubscribers: map[uint64]map[int64]struct{}{},
        subscriberQuery:  map[int64]uint64{},
        queryValues:      map[uint64]any{},
    }
}

func CanonicalQueryToken(path string, args map[string]any) (string, error) {
    canonical, err := canonicalJSON(args)
    if err != nil {
        return "", err
    }
    return fmt.Sprintf("%s|%s", path, canonical), nil
}

func (s *LocalSyncState) Subscribe(path string, args map[string]any) (queryID uint64, subscriberID int64, added bool, err error) {
    token, err := CanonicalQueryToken(path, args)
    if err != nil {
        return 0, 0, false, err
    }
    id, ok := s.tokenToQueryID[token]
    if !ok {
        s.nextQueryID++
        id = s.nextQueryID
        s.tokenToQueryID[token] = id
        s.querySubscribers[id] = map[int64]struct{}{}
        s.querySetVersion++
        added = true
    }
    s.nextSubscriberID++
    subscriberID = s.nextSubscriberID
    s.querySubscribers[id][subscriberID] = struct{}{}
    s.subscriberQuery[subscriberID] = id
    return id, subscriberID, added, nil
}

func (s *LocalSyncState) Unsubscribe(subscriberID int64) (queryID uint64, removed bool, err error) {
    id, ok := s.subscriberQuery[subscriberID]
    if !ok {
        return 0, false, fmt.Errorf("unknown subscriber id %d", subscriberID)
    }
    delete(s.subscriberQuery, subscriberID)
    delete(s.querySubscribers[id], subscriberID)
    if len(s.querySubscribers[id]) == 0 {
        delete(s.querySubscribers, id)
        for token, tokenID := range s.tokenToQueryID {
            if tokenID == id {
                delete(s.tokenToQueryID, token)
                break
            }
        }
        s.querySetVersion++
        removed = true
    }
    return id, removed, nil
}

func (s *LocalSyncState) QuerySetVersion() uint64 {
    return s.querySetVersion
}

func (s *LocalSyncState) IdentityVersion() uint64 {
    return s.identityVersion
}

func (s *LocalSyncState) SetAuthCallback(fetcher AuthFetcher) error {
    s.authFetcher = fetcher
    token, err := fetcher(false)
    if err != nil {
        return err
    }
    s.authToken = token
    s.identityVersion++
    return nil
}

func (s *LocalSyncState) RefreshAuthOnReconnect() error {
    if s.authFetcher == nil {
        return nil
    }
    token, err := s.authFetcher(true)
    if err != nil {
        return err
    }
    s.authToken = token
    s.identityVersion++
    return nil
}

func (s *LocalSyncState) SetAuthToken(token *string) {
    s.authToken = token
    s.identityVersion++
}

func (s *LocalSyncState) AuthToken() *string {
    return s.authToken
}

func (s *LocalSyncState) SetQueryValue(queryID uint64, value any) {
    s.queryValues[queryID] = value
}

func (s *LocalSyncState) ResultsBySubscriber() map[int64]any {
    out := map[int64]any{}
    for subscriberID, queryID := range s.subscriberQuery {
        out[subscriberID] = s.queryValues[queryID]
    }
    return out
}

func (s *LocalSyncState) UpdateObservedTimestamp(ts uint64) {
    if ts > s.observedTS {
        s.observedTS = ts
    }
}

func (s *LocalSyncState) ObservedTimestamp() uint64 {
    return s.observedTS
}

func canonicalJSON(v any) (string, error) {
    switch t := v.(type) {
    case nil:
        return "null", nil
    case bool:
        if t {
            return "true", nil
        }
        return "false", nil
    case string:
        b, _ := json.Marshal(t)
        return string(b), nil
    case float64:
        b, _ := json.Marshal(t)
        return string(b), nil
    case int:
        return fmt.Sprintf("%d", t), nil
    case int64:
        return fmt.Sprintf("%d", t), nil
    case []any:
        parts := make([]string, 0, len(t))
        for _, item := range t {
            c, err := canonicalJSON(item)
            if err != nil {
                return "", err
            }
            parts = append(parts, c)
        }
        return "[" + strings.Join(parts, ",") + "]", nil
    case map[string]any:
        keys := make([]string, 0, len(t))
        for k := range t {
            keys = append(keys, k)
        }
        sort.Strings(keys)
        parts := make([]string, 0, len(keys))
        for _, key := range keys {
            c, err := canonicalJSON(t[key])
            if err != nil {
                return "", err
            }
            kb, _ := json.Marshal(key)
            parts = append(parts, fmt.Sprintf("%s:%s", string(kb), c))
        }
        return "{" + strings.Join(parts, ",") + "}", nil
    case []byte:
        return fmt.Sprintf(`{"$bytes":%q}`, base64.StdEncoding.EncodeToString(t)), nil
    default:
        return canonicalTyped(v)
    }
}

// canonicalTyped canonicalises typed slices, maps, pointers and named scalars
// by normalising them to the base shapes handled by canonicalJSON.
func canonicalTyped(v any) (string, error) {
    n, err := NormalizeValue(v)
    if err != nil {
        return "", fmt.Errorf("unsupported canonical type %T: %w", v, err)
    }
    return canonicalJSON(n)
}
