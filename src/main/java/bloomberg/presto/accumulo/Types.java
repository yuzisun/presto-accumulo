/*
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package bloomberg.presto.accumulo;

import static com.google.common.base.Preconditions.checkArgument;
import static java.lang.String.format;

import com.facebook.presto.spi.type.StandardTypes;
import com.facebook.presto.spi.type.Type;

public final class Types {
    private Types() {
    }

    public static <A, B extends A> B checkType(A value, Class<B> target,
            String name) {
        if (value == null) {
            throw new NullPointerException(format("%s is null", name));
        }
        checkArgument(target.isInstance(value), "%s must be of type %s, not %s",
                name, target.getName(), value.getClass().getName());
        return target.cast(value);
    }

    public static boolean isArrayType(Type type) {
        return type.getTypeSignature().getBase().equals(StandardTypes.ARRAY);
    }

    public static boolean isMapType(Type type) {
        return type.getTypeSignature().getBase().equals(StandardTypes.MAP);
    }

    public static Type getElementType(Type type) {
        return type.getTypeParameters().get(0);
    }

    public static Type getKeyType(Type type) {
        return type.getTypeParameters().get(0);
    }

    public static Type getValueType(Type type) {
        return type.getTypeParameters().get(1);
    }
}
