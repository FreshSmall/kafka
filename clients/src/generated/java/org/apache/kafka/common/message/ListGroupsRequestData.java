/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

// THIS CODE IS AUTOMATICALLY GENERATED.  DO NOT EDIT.

package org.apache.kafka.common.message;

import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;
import java.util.NavigableMap;
import java.util.TreeMap;
import org.apache.kafka.common.errors.UnsupportedVersionException;
import org.apache.kafka.common.protocol.ApiMessage;
import org.apache.kafka.common.protocol.MessageUtil;
import org.apache.kafka.common.protocol.ObjectSerializationCache;
import org.apache.kafka.common.protocol.Readable;
import org.apache.kafka.common.protocol.Writable;
import org.apache.kafka.common.protocol.types.CompactArrayOf;
import org.apache.kafka.common.protocol.types.Field;
import org.apache.kafka.common.protocol.types.RawTaggedField;
import org.apache.kafka.common.protocol.types.RawTaggedFieldWriter;
import org.apache.kafka.common.protocol.types.Schema;
import org.apache.kafka.common.protocol.types.Struct;
import org.apache.kafka.common.protocol.types.Type;
import org.apache.kafka.common.utils.ByteUtils;

import static java.util.Map.Entry;
import static org.apache.kafka.common.protocol.types.Field.TaggedFieldsSection;


public class ListGroupsRequestData implements ApiMessage {
    private List<String> statesFilter;
    private List<RawTaggedField> _unknownTaggedFields;
    
    public static final Schema SCHEMA_0 =
        new Schema(
        );
    
    public static final Schema SCHEMA_1 = SCHEMA_0;
    
    public static final Schema SCHEMA_2 = SCHEMA_1;
    
    public static final Schema SCHEMA_3 =
        new Schema(
            TaggedFieldsSection.of(
            )
        );
    
    public static final Schema SCHEMA_4 =
        new Schema(
            new Field("states_filter", new CompactArrayOf(Type.COMPACT_STRING), "The states of the groups we want to list. If empty all groups are returned with their state."),
            TaggedFieldsSection.of(
            )
        );
    
    public static final Schema[] SCHEMAS = new Schema[] {
        SCHEMA_0,
        SCHEMA_1,
        SCHEMA_2,
        SCHEMA_3,
        SCHEMA_4
    };
    
    public ListGroupsRequestData(Readable _readable, short _version) {
        read(_readable, _version);
    }
    
    public ListGroupsRequestData(Struct _struct, short _version) {
        fromStruct(_struct, _version);
    }
    
    public ListGroupsRequestData() {
        this.statesFilter = new ArrayList<String>(0);
    }
    
    @Override
    public short apiKey() {
        return 16;
    }
    
    @Override
    public short lowestSupportedVersion() {
        return 0;
    }
    
    @Override
    public short highestSupportedVersion() {
        return 4;
    }
    
    @Override
    public void read(Readable _readable, short _version) {
        if (_version >= 4) {
            int arrayLength;
            arrayLength = _readable.readUnsignedVarint() - 1;
            if (arrayLength < 0) {
                throw new RuntimeException("non-nullable field statesFilter was serialized as null");
            } else {
                ArrayList<String> newCollection = new ArrayList<String>(arrayLength);
                for (int i = 0; i < arrayLength; i++) {
                    int length;
                    length = _readable.readUnsignedVarint() - 1;
                    if (length < 0) {
                        throw new RuntimeException("non-nullable field statesFilter element was serialized as null");
                    } else if (length > 0x7fff) {
                        throw new RuntimeException("string field statesFilter element had invalid length " + length);
                    } else {
                        newCollection.add(_readable.readString(length));
                    }
                }
                this.statesFilter = newCollection;
            }
        } else {
            this.statesFilter = new ArrayList<String>(0);
        }
        this._unknownTaggedFields = null;
        if (_version >= 3) {
            int _numTaggedFields = _readable.readUnsignedVarint();
            for (int _i = 0; _i < _numTaggedFields; _i++) {
                int _tag = _readable.readUnsignedVarint();
                int _size = _readable.readUnsignedVarint();
                switch (_tag) {
                    default:
                        this._unknownTaggedFields = _readable.readUnknownTaggedField(this._unknownTaggedFields, _tag, _size);
                        break;
                }
            }
        }
    }
    
    @Override
    public void write(Writable _writable, ObjectSerializationCache _cache, short _version) {
        int _numTaggedFields = 0;
        if (_version >= 4) {
            _writable.writeUnsignedVarint(statesFilter.size() + 1);
            for (String statesFilterElement : statesFilter) {
                {
                    byte[] _stringBytes = _cache.getSerializedValue(statesFilterElement);
                    _writable.writeUnsignedVarint(_stringBytes.length + 1);
                    _writable.writeByteArray(_stringBytes);
                }
            }
        } else {
            if (!statesFilter.isEmpty()) {
                throw new UnsupportedVersionException("Attempted to write a non-default statesFilter at version " + _version);
            }
        }
        RawTaggedFieldWriter _rawWriter = RawTaggedFieldWriter.forFields(_unknownTaggedFields);
        _numTaggedFields += _rawWriter.numFields();
        if (_version >= 3) {
            _writable.writeUnsignedVarint(_numTaggedFields);
            _rawWriter.writeRawTags(_writable, Integer.MAX_VALUE);
        } else {
            if (_numTaggedFields > 0) {
                throw new UnsupportedVersionException("Tagged fields were set, but version " + _version + " of this message does not support them.");
            }
        }
    }
    
    @SuppressWarnings("unchecked")
    @Override
    public void fromStruct(Struct struct, short _version) {
        NavigableMap<Integer, Object> _taggedFields = null;
        this._unknownTaggedFields = null;
        if (_version >= 3) {
            _taggedFields = (NavigableMap<Integer, Object>) struct.get("_tagged_fields");
        }
        if (_version >= 4) {
            Object[] _nestedObjects = struct.getArray("states_filter");
            this.statesFilter = new ArrayList<String>(_nestedObjects.length);
            for (Object nestedObject : _nestedObjects) {
                this.statesFilter.add((String) nestedObject);
            }
        } else {
            this.statesFilter = new ArrayList<String>(0);
        }
        if (_version >= 3) {
            if (!_taggedFields.isEmpty()) {
                this._unknownTaggedFields = new ArrayList<>(_taggedFields.size());
                for (Entry<Integer, Object> entry : _taggedFields.entrySet()) {
                    this._unknownTaggedFields.add((RawTaggedField) entry.getValue());
                }
            }
        }
    }
    
    @Override
    public Struct toStruct(short _version) {
        TreeMap<Integer, Object> _taggedFields = null;
        if (_version >= 3) {
            _taggedFields = new TreeMap<>();
        }
        Struct struct = new Struct(SCHEMAS[_version]);
        if (_version >= 4) {
            String[] _nestedObjects = new String[statesFilter.size()];
            int i = 0;
            for (String element : this.statesFilter) {
                _nestedObjects[i++] = element;
            }
            struct.set("states_filter", (Object[]) _nestedObjects);
        } else {
            if (!statesFilter.isEmpty()) {
                throw new UnsupportedVersionException("Attempted to write a non-default statesFilter at version " + _version);
            }
        }
        if (_version >= 3) {
            struct.set("_tagged_fields", _taggedFields);
        }
        return struct;
    }
    
    @Override
    public int size(ObjectSerializationCache _cache, short _version) {
        int _size = 0, _numTaggedFields = 0;
        if (_version >= 4) {
            {
                int _arraySize = 0;
                _arraySize += ByteUtils.sizeOfUnsignedVarint(statesFilter.size() + 1);
                for (String statesFilterElement : statesFilter) {
                    byte[] _stringBytes = statesFilterElement.getBytes(StandardCharsets.UTF_8);
                    if (_stringBytes.length > 0x7fff) {
                        throw new RuntimeException("'statesFilterElement' field is too long to be serialized");
                    }
                    _cache.cacheSerializedValue(statesFilterElement, _stringBytes);
                    _arraySize += _stringBytes.length + ByteUtils.sizeOfUnsignedVarint(_stringBytes.length + 1);
                }
                _size += _arraySize;
            }
        }
        if (_unknownTaggedFields != null) {
            _numTaggedFields += _unknownTaggedFields.size();
            for (RawTaggedField _field : _unknownTaggedFields) {
                _size += ByteUtils.sizeOfUnsignedVarint(_field.tag());
                _size += ByteUtils.sizeOfUnsignedVarint(_field.size());
                _size += _field.size();
            }
        }
        if (_version >= 3) {
            _size += ByteUtils.sizeOfUnsignedVarint(_numTaggedFields);
        } else {
            if (_numTaggedFields > 0) {
                throw new UnsupportedVersionException("Tagged fields were set, but version " + _version + " of this message does not support them.");
            }
        }
        return _size;
    }
    
    @Override
    public boolean equals(Object obj) {
        if (!(obj instanceof ListGroupsRequestData)) return false;
        ListGroupsRequestData other = (ListGroupsRequestData) obj;
        if (this.statesFilter == null) {
            if (other.statesFilter != null) return false;
        } else {
            if (!this.statesFilter.equals(other.statesFilter)) return false;
        }
        return true;
    }
    
    @Override
    public int hashCode() {
        int hashCode = 0;
        hashCode = 31 * hashCode + (statesFilter == null ? 0 : statesFilter.hashCode());
        return hashCode;
    }
    
    @Override
    public ListGroupsRequestData duplicate() {
        ListGroupsRequestData _duplicate = new ListGroupsRequestData();
        ArrayList<String> newStatesFilter = new ArrayList<String>(statesFilter.size());
        for (String _element : statesFilter) {
            newStatesFilter.add(_element);
        }
        _duplicate.statesFilter = newStatesFilter;
        return _duplicate;
    }
    
    @Override
    public String toString() {
        return "ListGroupsRequestData("
            + "statesFilter=" + MessageUtil.deepToString(statesFilter.iterator())
            + ")";
    }
    
    public List<String> statesFilter() {
        return this.statesFilter;
    }
    
    @Override
    public List<RawTaggedField> unknownTaggedFields() {
        if (_unknownTaggedFields == null) {
            _unknownTaggedFields = new ArrayList<>(0);
        }
        return _unknownTaggedFields;
    }
    
    public ListGroupsRequestData setStatesFilter(List<String> v) {
        this.statesFilter = v;
        return this;
    }
}
