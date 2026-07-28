package driver

import (
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"sync"

	"github.com/thesyncim/vibedb/store"
	"github.com/thesyncim/vibedb/store/durable"
)

const catalogVersion = 1

type catalogFile struct {
	Version int                   `json:"version"`
	Tables  map[string]*tableMeta `json:"tables"`
}

type tableMeta struct {
	PrimaryKey string      `json:"primary_key"`
	Indexes    []indexMeta `json:"indexes,omitempty"`
}

type indexMeta struct {
	Name  string   `json:"name"`
	Paths []string `json:"paths"`
}

type table struct {
	meta       *tableMeta
	file       *os.File
	collection *durable.Collection
}

type database struct {
	mu      sync.RWMutex
	path    string
	dataDir string
	catalog catalogFile
	tables  map[string]*table
	closed  bool
}

func openDatabase(path string) (*database, error) {
	if path == "" {
		return nil, errors.New("vibedb: the DSN must be a file path")
	}
	absolute, err := filepath.Abs(path)
	if err != nil {
		return nil, err
	}
	d := &database{
		path: absolute, dataDir: absolute + ".tables",
		catalog: catalogFile{Version: catalogVersion, Tables: make(map[string]*tableMeta)},
		tables:  make(map[string]*table),
	}
	raw, err := os.ReadFile(absolute)
	if err != nil && !os.IsNotExist(err) {
		return nil, err
	}
	if err == nil {
		if err := json.Unmarshal(raw, &d.catalog); err != nil {
			return nil, fmt.Errorf("vibedb: read SQL catalog %s: %w", absolute, err)
		}
		if d.catalog.Version != catalogVersion || d.catalog.Tables == nil {
			return nil, fmt.Errorf("vibedb: unsupported SQL catalog version %d", d.catalog.Version)
		}
	}
	for name, meta := range d.catalog.Tables {
		t := &table{meta: meta}
		dataPath := d.tablePath(name)
		file, openErr := os.OpenFile(dataPath, os.O_RDWR, 0)
		if os.IsNotExist(openErr) {
			d.tables[name] = t
			continue
		}
		if openErr != nil {
			_ = d.close()
			return nil, openErr
		}
		collection, openErr := durable.Open(file, durableOptions(meta))
		if openErr != nil {
			_ = file.Close()
			_ = d.close()
			return nil, fmt.Errorf("vibedb: open table %q: %w", name, openErr)
		}
		t.file, t.collection = file, collection
		d.tables[name] = t
	}
	return d, nil
}

func durableOptions(meta *tableMeta) durable.Options {
	options := durable.Options{DocumentFormat: durable.DocumentFormatVerbatim}
	for _, index := range meta.Indexes {
		options.Indexes = append(options.Indexes, store.IndexDefinition{
			Name: index.Name, Paths: append([]string(nil), index.Paths...),
		})
	}
	return options
}

func (d *database) tablePath(name string) string {
	return filepath.Join(d.dataDir, hex.EncodeToString([]byte(name))+".vjc")
}

func (d *database) persistCatalogLocked() error {
	if err := os.MkdirAll(filepath.Dir(d.path), 0o700); err != nil {
		return err
	}
	raw, err := json.MarshalIndent(d.catalog, "", "  ")
	if err != nil {
		return err
	}
	tmp, err := os.CreateTemp(filepath.Dir(d.path), "."+filepath.Base(d.path)+".tmp-*")
	if err != nil {
		return err
	}
	tmpName := tmp.Name()
	ok := false
	defer func() {
		_ = tmp.Close()
		if !ok {
			_ = os.Remove(tmpName)
		}
	}()
	if err := tmp.Chmod(0o600); err != nil {
		return err
	}
	if _, err := tmp.Write(raw); err != nil {
		return err
	}
	if err := tmp.Sync(); err != nil {
		return err
	}
	if err := tmp.Close(); err != nil {
		return err
	}
	if err := os.Rename(tmpName, d.path); err != nil {
		return err
	}
	ok = true
	return nil
}

type seedDocument struct {
	key      string
	document []byte
}

func (d *database) materializeLocked(name string, documents []seedDocument) (*table, error) {
	t, ok := d.tables[name]
	if !ok {
		return nil, fmt.Errorf("vibedb: table %q does not exist", name)
	}
	if t.collection != nil {
		return t, nil
	}
	if err := os.MkdirAll(d.dataDir, 0o700); err != nil {
		return nil, err
	}
	path := d.tablePath(name)
	file, err := os.OpenFile(path, os.O_RDWR|os.O_CREATE|os.O_EXCL, 0o600)
	if err != nil {
		return nil, err
	}
	options := durableOptions(t.meta)
	var collection *durable.Collection
	if len(t.meta.Indexes) == 0 {
		collection, err = durable.Create(file, options)
		if err == nil {
			for _, document := range documents {
				_, err = collection.Put(document.key, document.document)
				if err != nil {
					break
				}
			}
		}
	} else {
		var heap *store.Collection
		heap, err = store.New(store.Options{})
		if err == nil {
			for _, document := range documents {
				_, err = heap.Put(document.key, document.document)
				if err != nil {
					break
				}
			}
		}
		if err == nil {
			_, err = durable.CreateFromPrimary(heap, file, options)
		}
		if err == nil {
			collection, err = durable.Open(file, options)
		}
	}
	if err != nil {
		if collection != nil {
			_ = collection.Close()
		}
		_ = file.Close()
		_ = os.Remove(path)
		return nil, err
	}
	t.file, t.collection = file, collection
	return t, nil
}

func (d *database) close() error {
	d.mu.Lock()
	defer d.mu.Unlock()
	if d.closed {
		return nil
	}
	d.closed = true
	var result error
	for _, t := range d.tables {
		if t.collection != nil {
			result = errors.Join(result, t.collection.Close())
		}
		if t.file != nil {
			result = errors.Join(result, t.file.Close())
		}
	}
	return result
}
