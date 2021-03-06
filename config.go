package gotickfile

import (
	"fmt"
	"reflect"
)

type TickFileConfig func(file *TickFile)

func WithDataType(typ reflect.Type) TickFileConfig {
	return func(tf *TickFile) {
		tf.dataType = typ
		itemSection := ItemSection{}
		itemSection.Info.ItemSize = uint32(typ.Size())
		itemSection.Info.ItemTypeName = typ.Name()
		switch typ.Kind() {
		case reflect.Struct:
			fIdx := 0
			for i := 0; i < typ.NumField(); i++ {
				dataField := typ.Field(i)
				itemField := ItemSectionField{}
				itemField.Name = dataField.Name
				itemField.Offset = uint32(dataField.Offset)
				itemField.Index = uint32(fIdx)
				itemField.Type = kindToFieldType[dataField.Type.Kind()]
				itemSection.Fields = append(itemSection.Fields, itemField)

				fIdx += 1
			}
			itemSection.Info.FieldCount = uint32(fIdx)

		case reflect.Uint64:
			itemSection.Info.FieldCount = 1
			itemField := ItemSectionField{}
			itemField.Name = typ.Name()
			itemField.Offset = 0
			itemField.Index = 0
			itemField.Type = kindToFieldType[reflect.Uint64]
			itemSection.Fields = append(itemSection.Fields, itemField)

		case reflect.Int64:
			itemSection.Info.FieldCount = 1
			itemField := ItemSectionField{}
			itemField.Name = typ.Name()
			itemField.Offset = 0
			itemField.Index = 0
			itemField.Type = kindToFieldType[reflect.Int64]
			itemSection.Fields = append(itemSection.Fields, itemField)

		case reflect.Float64:
			itemSection.Info.FieldCount = 1
			itemField := ItemSectionField{}
			itemField.Name = typ.Name()
			itemField.Offset = 0
			itemField.Index = 0
			itemField.Type = kindToFieldType[reflect.Float64]
			itemSection.Fields = append(itemSection.Fields, itemField)

		default:
			panic(fmt.Sprintf("unsupported type: %s", typ.String()))

		}

		tf.itemSection = &itemSection
	}
}

func WithBasicType(typ reflect.Type) TickFileConfig {
	return func(tf *TickFile) {
		tf.dataType = typ
		itemSection := ItemSection{}
		itemSection.Info.FieldCount = 1
		itemSection.Info.ItemSize = uint32(typ.Size())
		itemSection.Info.ItemTypeName = typ.Name()
		itemField := ItemSectionField{}
		itemField.Name = typ.Name()
		itemField.Offset = 0
		itemField.Index = 0
		ok := true
		itemField.Type, ok = kindToFieldType[typ.Kind()]
		if !ok {
			panic(fmt.Sprintf("unsupported type: %s", typ.String()))
		}
		itemSection.Fields = append(itemSection.Fields, itemField)
		tf.itemSection = &itemSection
	}
}

func WithContentDescription(description string) TickFileConfig {
	return func(tf *TickFile) {
		tf.contentDescriptionSection = &ContentDescriptionSection{
			ContentDescription: description,
		}
	}
}

func WithNameValues(nameValues map[string]interface{}) TickFileConfig {
	return func(tf *TickFile) {
		tf.nameValueSection = &NameValueSection{
			NameValues: nameValues,
		}
	}
}

func WithTags(tags map[string]string) TickFileConfig {
	return func(tf *TickFile) {
		tf.tagsSection = &TagsSection{
			Tags: tags,
		}
	}
}
