package core

import (
	"encoding/json"

	"github.com/prabhatsharma/zinc/pkg/storage"
)

// UpdateDocument inserts or updates a document in the zinc index
func (index *Index) UpdateDocument(docID string, doc map[string]interface{}, mintedID bool) error {
	bdoc, err := index.BuildBlugeDocumentFromJSON(docID, doc)
	if err != nil {
		return err
	}

	// storage source field
	if err := index.SetSourceData(docID, doc); err != nil {
		return err
	}

	// Finally update the document on disk
	writer := index.Writer
	if !mintedID {
		err = writer.Update(bdoc.ID(), bdoc)
	} else {
		err = writer.Insert(bdoc)
		index.GainDocsCount(1)
	}
	return err
}

func (index *Index) SetSourceData(docID string, sourceDoc map[string]interface{}) error {
	indexDB, err := storage.Cli.GetIndex(index.Name)
	if err != nil {
		return err
	}
	jdoc, err := json.Marshal(sourceDoc)
	if err != nil {
		return err
	}
	return indexDB.Set(docID, jdoc)
}

func (index *Index) GetSourceData(docID string) ([]byte, error) {
	indexStorage, err := storage.Cli.GetIndex(index.Name)
	if err != nil {
		return nil, err
	}
	return indexStorage.Get(docID)
}

func (index *Index) DeleteSourceData(docID string) error {
	indexStorage, err := storage.Cli.GetIndex(index.Name)
	if err != nil {
		return err
	}
	return indexStorage.Delete(docID)
}
