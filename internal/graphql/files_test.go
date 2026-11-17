package graphql

import (
	"context"
	"testing"

	"github.com/sarna/worb/internal/store"
)

// TestReadFilesSignsExistingObject 确认查询侧 url 固定为 upload 路径，有 object_key 时 directUrl 现签。
// TestReadFilesSignsExistingObject checks that query url stays on the upload path and directUrl is signed when an object key exists.
func TestReadFilesSignsExistingObject(t *testing.T) {
	r := &Resolver{Sign: func(ctx context.Context, key string) (string, error) {
		// Agent: 只签非空 key，测试确认没有把空 key 送进来。
		if key != "obj" {
			t.Fatalf("sign key %s", key)
		}
		return "https://signed.example/obj", nil
	}}
	conn := r.readFiles(context.Background(), []store.VisibleFile{
		{ID: "run-1", Name: "a.log", ObjectKey: "obj"},
		{ID: "run-2", Name: "b.log"},
	})
	if len(conn.Edges) != 2 {
		t.Fatalf("edges %d", len(conn.Edges))
	}
	signed := conn.Edges[0].Node
	if signed.URL == nil || *signed.URL != "/files/upload/run-1" || signed.DirectURL == nil || *signed.DirectURL != "https://signed.example/obj" {
		t.Fatalf("signed %+v", signed)
	}
	plain := conn.Edges[1].Node
	if plain.URL == nil || plain.DirectURL == nil || *plain.URL != "/files/upload/run-2" || *plain.DirectURL != *plain.URL {
		t.Fatalf("plain %+v", plain)
	}
	if conn.PageInfo == nil || conn.PageInfo.HasNextPage || conn.Edges[0].Cursor != "run-1" {
		t.Fatalf("page %+v", conn.PageInfo)
	}
}

func TestFilePageAndNameFilter(t *testing.T) {
	pattern := "%.csv"
	if !fileNameSelected("test.csv", nil, &pattern) || fileNameSelected("a.log", nil, &pattern) {
		t.Fatal("like")
	}
	if !fileNameSelected("test.csv", []string{"test.csv"}, nil) || fileNameSelected("a.log", []string{"test.csv"}, nil) {
		t.Fatal("names")
	}
	files := []store.VisibleFile{{ID: "run-1", Name: "a"}, {ID: "run-2", Name: "b"}, {ID: "run-3", Name: "c"}}
	first := 1
	page, more := pageVisibleFiles(files, nil, &first)
	if len(page) != 1 || page[0].ID != "run-1" || !more {
		t.Fatalf("first page %+v %v", page, more)
	}
	after := "run-1"
	page, more = pageVisibleFiles(files, &after, &first)
	if len(page) != 1 || page[0].ID != "run-2" || !more {
		t.Fatalf("next page %+v %v", page, more)
	}
}
