#include <memory>
#include <string>
#include <functional>
#include <type_traits>
#include <vector>
#include <tuple>
#include <unordered_map>
#include <sstream>
#include <ostream>
#include <iostream>
#include <cmath>

namespace js
{

#define OR(x, y) ((bool)(x) ? (x) : (y))
#define AND(x, y) ((bool)(x) ? (y) : (x))

struct any;
struct object;

struct undefined {

    undefined () {
    }

    inline operator bool() {
        return false;
    }

    friend std::ostream& operator << (std::ostream& os, undefined val)
    {
        return os << "undefined";
    }       
};

struct boolean {

    bool _value;

    boolean (bool initValue) {
        _value = initValue;
    }

    inline operator bool() {
        return _value;
    }

    friend std::ostream& operator << (std::ostream& os, boolean val)
    {
        return os << ((bool)val ? "true" : "false");
    }       
};

struct number {

    double _value;

    number() : _value(0) {
    }

    template<class T, class = std::enable_if<std::is_integral_v<T>>>
    number (T initValue) {
        _value = static_cast<T>(initValue);
    }

    template<class T, class = std::enable_if<std::is_integral_v<T>>>
    inline operator T() const {
        return static_cast<T>(_value);
    }

    operator std::string() const {
        return std::to_string(_value);
    }    

    operator size_t() const {
        return (size_t)_value;
    }    

    friend std::ostream& operator << (std::ostream& os, number val)
    {
        return os << val._value;
    }       
};

struct string {

    std::string _value;

    string () : _value(nullptr) {
    }    

    string (std::string value) : _value(value) {
    }

    inline operator const char*() {
        return _value.c_str();
    }

    template<class T, class = std::enable_if<std::is_integral_v<T>>>
    string& operator+ (T t) {
        _value.append(std::to_string(t));
        return *this;
    }

    string& operator+ (number value) {
        _value.append(value.operator std::string());
        return *this;
    }

    string& operator+ (string value) {
        _value.append(value._value);
        return *this;
    }

    friend std::ostream& operator << (std::ostream& os, string val)
    {
        return os << val._value;
    }    
};

struct array {

    std::vector<any> _values;

    array ();

    array (std::initializer_list<any> values);

    template<class T, class = std::enable_if<std::is_integral_v<T> || std::is_same_v<T, number>>>
    any& operator[] (T t);

    friend std::ostream& operator << (std::ostream& os, array val)
    {
        return os << "[array]";
    }
};

struct object {

    using pair = std::pair<std::string, any>;

    std::unordered_map<std::string, any> _values;

    object ();

    object (std::initializer_list<pair> values);

    template<class T, class = std::enable_if<std::is_integral_v<T> || std::is_same_v<T, number>>>
    any& operator[] (T t);

    any& operator[] (const char* s);

    any& operator[] (std::string s);

    friend std::ostream& operator << (std::ostream& os, object val)
    {
        return os << "[object]";
    }
};

struct any {

    enum anyTypeId {
        undefined,
        boolean,
        number,
        string,
        array,
        object
    };

    union anyType  {
        js::boolean _boolean;
        js::number _number;
        void* _data;

        constexpr anyType(): _data(nullptr) {
        }

        template<class T, class = std::enable_if<std::is_integral_v<T>>>
        constexpr anyType(T t): _number(t) {
        }     

        constexpr anyType(std::nullptr_t): _data(nullptr) {
        }

        constexpr anyType(void* ptr): _data(ptr) {
        }        
    };

    anyTypeId _type;
    anyType _value;

    any() : _type(anyTypeId::undefined) {
    }

    template<class T, class = std::enable_if<std::is_integral_v<T>>>
    any(T initValue) : _type(anyTypeId::number), _value((T)initValue) {
    }

    any(bool value) : _type(anyTypeId::boolean), _value(value) {
    }

    any(js::boolean value) : _type(anyTypeId::boolean), _value(value) {
    }

    any(js::number value) : _type(anyTypeId::number), _value(value) {
    }

    any(const js::string& value) : _type(anyTypeId::string), _value((void*)new js::string(value)) {
    }

    any(const js::array& value) : _type(anyTypeId::array), _value((void*)new js::array(value)) {
    }   

    any(const js::object& value) : _type(anyTypeId::object), _value((void*)new js::object(value)) {
    }   

    template<class T>
    any& operator[] (T t) {
        if (_type == anyTypeId::array) {
            return (*(js::array*)_value._data)[t];
        }

        if (_type == anyTypeId::object) {
            return (*(js::object*)_value._data)[t];
        }

        throw "wrong type";
    }    

    operator string() {
        if (_type == anyTypeId::string) {
            return *(js::string*)_value._data;
        }

        throw "wrong type";        
    } 

    operator bool() {
        return _type != anyTypeId::undefined;
    }

    friend std::ostream& operator << (std::ostream& os, any val)
    {
        if (val._type == anyTypeId::undefined) {
            return os << "undefined";
        }

        if (val._type == anyTypeId::boolean) {
            return os << val._value._boolean;
        }

        if (val._type == anyTypeId::number) {
            return os << val._value._number;
        }

        if (val._type == anyTypeId::string) {
            return os << *(js::string*)val._value._data;
        }

        return os << "[any]";
    }    
};

// Array
array::array() : _values() {
}

array::array (std::initializer_list<any> values) : _values(values) {
}

template<class T, class>
any& array::operator[] (T t) {
    return _values[(size_t)t];
}

// End of Array

// Object
object::object() : _values() {
}

object::object (std::initializer_list<pair> values) {
    for (auto& item : values) {
        _values[item.first] = item.second;
    }
}

template<class T, class>
any& object::operator[] (T t) {
    return _values[std::to_string(t)];
}

any& object::operator[] (const char* s) {
    return _values[std::string(s)];
}

any& object::operator[] (std::string s) {
    return _values[s];
}

// End of Object
template < typename T >
struct Element {
    bool _undefined;
    T _t;
    Element() : _undefined(true), _t(T())  {}
    Element(const T t) : _undefined(false), _t(t)  {}

    inline operator bool() {
        return !_undefined;
    }

    template<class I>
    inline decltype(_t[I()]) operator[] (I i) {
        return _t[i];
    }     

    friend std::ostream& operator << (std::ostream& os, Element<T> val)
    {
        os << "item:"; 
        if (_undefined) {
            return os << "undefined";    
        }

        return os << val._t;
    }        
};

template < typename T >
struct ElementReference {
    bool _undefined;
    T& _t;
    ElementReference() : _undefined(true), _t(T())  {}
    ElementReference(const T& t) : _undefined(false), _t(t)  {}

    inline operator bool() {
        return !_undefined;
    }

    inline operator T&() {
        return _t;
    }

    template<class I>
    inline decltype(_t[I()])& operator[] (I i) {
        return _t[i];
    }     

    friend std::ostream& operator << (std::ostream& os, ElementReference<T> val)
    {
        os << "ref:"; 
        if (_undefined) {
            return os << "undefined";    
        }

        return os << val._t;
    }    
};

template < typename T >
struct ReadOnlyArray {
    number length;
    std::vector<T> _values;

    ReadOnlyArray(std::initializer_list<T> values) : _values(values) {
    }

    template<class I, class = std::enable_if<std::is_integral_v<I> || std::is_same_v<I, number>>>
    Element<T> operator[] (I i) const {
        if ((size_t)i >= _values.size()) {
            return Element<T>();
        }

        return Element<T>(_values[(size_t)i]);
    }
};

template < typename T >
struct Array : public ReadOnlyArray<T> {
    Array(std::initializer_list<T> values) : ReadOnlyArray<T>(values) {
    }

    template<class I, class = std::enable_if<std::is_integral_v<I> || std::is_same_v<I, number>>>
    ElementReference<T&> operator[] (I i) {
        if ((size_t)i >= _values.size()) {
            return ElementReference<T&>();
        }

        return ElementReference<T&>(_values[(size_t)i]);
    }
};

string operator ""_S(const char* s, std::size_t size) {
    return string(s);
}


std::ostream& operator << (std::ostream& os, std::nullptr_t ptr)
{
    return os << "null";
}    

static struct Console
{
    Console() {
        std::cout << std::boolalpha;
    }

    template<class ... Args>
    void log(Args ... args) 
    {
        for (auto& arg : {args...}) 
        {
            std::cout << arg;
        }

        std::cout << std::endl;
    }

} console;

} // namespace js
