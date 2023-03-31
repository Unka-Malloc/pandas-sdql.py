
#include <Python.h>
#include "numpy/arrayobject.h"
#include "/usr/local/lib/python3.8/dist-packages/sdqlpy-1.0.0-py3.8.egg/sdqlpy/include/headers.h"

////////////////////////////////////

typedef struct {
    PyObject_HEAD
    phmap::flat_hash_map<tuple<VarChar<1>, VarChar<1>, double, double, double, double, double, double, double, double>, bool>* dict;
} FastDict_s1s1ffffffff_b;

////////////////////////////////////

typedef struct {
    PyObject_HEAD
    phmap::flat_hash_map<tuple<double, VarChar<25>, VarChar<25>, long, VarChar<25>, VarChar<40>, VarChar<15>, VarChar<101>>, bool>* dict;
} FastDict_fs25s25is25s40s15s101_b;

////////////////////////////////////

typedef struct {
    PyObject_HEAD
    phmap::flat_hash_map<tuple<long, long, long, double>, bool>* dict;
} FastDict_iiif_b;

////////////////////////////////////

typedef struct {
    PyObject_HEAD
    phmap::flat_hash_map<tuple<VarChar<15>, double>, bool>* dict;
} FastDict_s15f_b;

////////////////////////////////////

typedef struct {
    PyObject_HEAD
    phmap::flat_hash_map<tuple<VarChar<25>, double>, bool>* dict;
} FastDict_s25f_b;

////////////////////////////////////

typedef struct {
    PyObject_HEAD
    phmap::flat_hash_map<tuple<double>, bool>* dict;
} FastDict_f_b;

////////////////////////////////////

typedef struct {
    PyObject_HEAD
    phmap::flat_hash_map<tuple<VarChar<25>, VarChar<25>, long, double>, bool>* dict;
} FastDict_s25s25if_b;

////////////////////////////////////

typedef struct {
    PyObject_HEAD
    phmap::flat_hash_map<tuple<long, double>, bool>* dict;
} FastDict_if_b;

////////////////////////////////////

typedef struct {
    PyObject_HEAD
    phmap::flat_hash_map<tuple<VarChar<25>, long, double>, bool>* dict;
} FastDict_s25if_b;

////////////////////////////////////

typedef struct {
    PyObject_HEAD
    phmap::flat_hash_map<tuple<long, VarChar<25>, double, VarChar<15>, VarChar<25>, VarChar<40>, VarChar<117>, double>, bool>* dict;
} FastDict_is25fs15s25s40s117f_b;

////////////////////////////////////

typedef struct {
    PyObject_HEAD
    phmap::flat_hash_map<tuple<VarChar<10>, long, long>, bool>* dict;
} FastDict_s10ii_b;

////////////////////////////////////

typedef struct {
    PyObject_HEAD
    phmap::flat_hash_map<tuple<long, VarChar<25>, VarChar<40>, VarChar<15>, double>, bool>* dict;
} FastDict_is25s40s15f_b;

////////////////////////////////////

typedef struct {
    PyObject_HEAD
    phmap::flat_hash_map<tuple<VarChar<10>, VarChar<25>, long, long>, bool>* dict;
} FastDict_s10s25ii_b;

////////////////////////////////////

typedef struct {
    PyObject_HEAD
    phmap::flat_hash_map<tuple<VarChar<25>, long, long, long, double, double>, bool>* dict;
} FastDict_s25iiiff_b;

////////////////////////////////////

typedef struct {
    PyObject_HEAD
    phmap::flat_hash_map<tuple<VarChar<25>, VarChar<40>>, bool>* dict;
} FastDict_s25s40_b;

////////////////////////////////////

typedef struct {
    PyObject_HEAD
    phmap::flat_hash_map<tuple<VarChar<2>, double, double>, bool>* dict;
} FastDict_s2ff_b;

////////////////////////////////////



int init_numpy(){import_array();return 0;}

/*
static string GetType(PyObject *obj)
{
    PyTypeObject* type = obj->ob_type;
    const char* p = type->tp_name;
    return string(p);
}
*/

static wchar_t* ConstantString(const char* data, int len)
{
    wchar_t* wc = new wchar_t[len]; 
    mbstowcs (wc, data, len); 
    return wc;
} 

using namespace std;

tbb::task_scheduler_init scheduler(4);



class DB
{
    public:
int li_dataset_size = 0;
int cu_dataset_size = 0;
int ord_dataset_size = 0;
int na_dataset_size = 0;
int re_dataset_size = 0;
int pa_dataset_size = 0;
int ps_dataset_size = 0;
int su_dataset_size = 0;

};

static PyObject * q1(PyObject *self, PyObject* args)
{
	

    PyObject * db_;
    if (!PyArg_ParseTuple(args, "O", &db_)){
        PyErr_SetString(PyExc_ValueError,"Error while parsing the sum inputs.");
        return NULL;
    }
    
    
    DB dbobj;
    DB* db = &dbobj;
    
const static int numpy_initialized =  init_numpy();


	auto li_size = PyArray_Size(PyList_GetItem(PyList_GetItem(db_, 0), 0));
	db->li_dataset_size = li_size;
	auto l_orderkey = (long*)PyArray_DATA(PyList_GetItem(PyList_GetItem(db_, 0), 0));
	auto l_partkey = (long*)PyArray_DATA(PyList_GetItem(PyList_GetItem(db_, 0), 1));
	auto l_suppkey = (long*)PyArray_DATA(PyList_GetItem(PyList_GetItem(db_, 0), 2));
	auto l_linenumber = (long*)PyArray_DATA(PyList_GetItem(PyList_GetItem(db_, 0), 3));
	auto l_quantity = (double*)PyArray_DATA(PyList_GetItem(PyList_GetItem(db_, 0), 4));
	auto l_extendedprice = (double*)PyArray_DATA(PyList_GetItem(PyList_GetItem(db_, 0), 5));
	auto l_discount = (double*)PyArray_DATA(PyList_GetItem(PyList_GetItem(db_, 0), 6));
	auto l_tax = (double*)PyArray_DATA(PyList_GetItem(PyList_GetItem(db_, 0), 7));
	auto l_returnflag= (VarChar<1>*)PyArray_DATA(PyList_GetItem(PyList_GetItem(db_, 0), 8));
	auto l_linestatus= (VarChar<1>*)PyArray_DATA(PyList_GetItem(PyList_GetItem(db_, 0), 9));
	auto l_shipdate = (long*)PyArray_DATA(PyList_GetItem(PyList_GetItem(db_, 0), 10));
	auto l_commitdate = (long*)PyArray_DATA(PyList_GetItem(PyList_GetItem(db_, 0), 11));
	auto l_receiptdate = (long*)PyArray_DATA(PyList_GetItem(PyList_GetItem(db_, 0), 12));
	auto l_shipinstruct= (VarChar<25>*)PyArray_DATA(PyList_GetItem(PyList_GetItem(db_, 0), 13));
	auto l_shipmode= (VarChar<10>*)PyArray_DATA(PyList_GetItem(PyList_GetItem(db_, 0), 14));
	auto l_comment= (VarChar<44>*)PyArray_DATA(PyList_GetItem(PyList_GetItem(db_, 0), 15));
	auto l_NA= (VarChar<1>*)PyArray_DATA(PyList_GetItem(PyList_GetItem(db_, 0), 16));



	
auto v12 = phmap::flat_hash_map<tuple<long,long,long,long,double,double,double,double,VarChar<1>,VarChar<1>,long,long,long,VarChar<25>,VarChar<10>,VarChar<44>,VarChar<1>>,bool>({});
tbb::enumerable_thread_specific<phmap::flat_hash_map<tuple<long,long,long,long,double,double,double,double,VarChar<1>,VarChar<1>,long,long,long,VarChar<25>,VarChar<10>,VarChar<44>,VarChar<1>>,bool>> v25;
auto v14 = phmap::flat_hash_map<tuple<long,long,long,long,double,double,double,double,VarChar<1>,VarChar<1>,long,long,long,VarChar<25>,VarChar<10>,VarChar<44>,VarChar<1>,double>,bool>({});
auto v16 = phmap::flat_hash_map<tuple<long,long,long,long,double,double,double,double,VarChar<1>,VarChar<1>,long,long,long,VarChar<25>,VarChar<10>,VarChar<44>,VarChar<1>,double,double>,bool>({});
auto v18 = phmap::flat_hash_map<tuple<VarChar<1>,VarChar<1>>,tuple<double,double,double,double,double,double,double,double,double,double,double>>({});
auto v20 = phmap::flat_hash_map<tuple<VarChar<1>,VarChar<1>,double,double,double,double,double,double,double,double>,bool>({});	
	

auto v21 = db->li_dataset_size; tbb::parallel_for(tbb::blocked_range<size_t>(0, v21), [&](const tbb::blocked_range<size_t>& v22){auto& v26=v25.local();for (size_t v11=v22.begin(), end=v22.end(); v11!=end; ++v11){if ((l_shipdate[v11] <= (long)19980902)){v26[make_tuple(l_orderkey[v11], l_partkey[v11], l_suppkey[v11], l_linenumber[v11], l_quantity[v11], l_extendedprice[v11], l_discount[v11], l_tax[v11], l_returnflag[v11], l_linestatus[v11], l_shipdate[v11], l_commitdate[v11], l_receiptdate[v11], l_shipinstruct[v11], l_shipmode[v11], l_comment[v11], l_NA[v11])] += true;};}});for (auto& local : v25)AddMap<phmap::flat_hash_map<tuple<long,long,long,long,double,double,double,double,VarChar<1>,VarChar<1>,long,long,long,VarChar<25>,VarChar<10>,VarChar<44>,VarChar<1>>,bool>,tuple<long,long,long,long,double,double,double,double,VarChar<1>,VarChar<1>,long,long,long,VarChar<25>,VarChar<10>,VarChar<44>,VarChar<1>>,bool>(v12, local);const auto& lineitem_0 = v12;

for (auto& v13 : lineitem_0){v14[tuple_cat((v13.first),move(make_tuple(( /* l_extendedprice */get<5>((v13.first)) * (1.0 -  /* l_discount */get<6>((v13.first)))))))] += (v13.second);}const auto& lineitem_1 = v14;

for (auto& v15 : lineitem_1){v16[tuple_cat((v15.first),move(make_tuple((( /* l_extendedprice */get<5>((v15.first)) * (1.0 -  /* l_discount */get<6>((v15.first)))) * (1.0 +  /* l_tax */get<7>((v15.first)))))))] += (v15.second);}const auto& lineitem_2 = v16;

for (auto& v17 : lineitem_2){v18[make_tuple( /* l_returnflag */get<8>((v17.first)), /* l_linestatus */get<9>((v17.first)))] += make_tuple( /* l_quantity */get<4>((v17.first)), /* l_extendedprice */get<5>((v17.first)), /* disc_price */get<17>((v17.first)), /* charge */get<18>((v17.first)), /* l_quantity */get<4>((v17.first)),1.0, /* l_extendedprice */get<5>((v17.first)),1.0, /* l_discount */get<6>((v17.first)),1.0,(( /* l_quantity */get<4>((v17.first)) != none)) ? (1.0) : (0.0));}const auto& lineitem_3 = v18;

for (auto& v19 : lineitem_3){v20[make_tuple( /* l_returnflag */get<0>((v19.first)), /* l_linestatus */get<1>((v19.first)), /* sum_qty */get<0>((v19.second)), /* sum_base_price */get<1>((v19.second)), /* sum_disc_price */get<2>((v19.second)), /* sum_charge */get<3>((v19.second)),( /* avg_qty_sum_for_mean */get<4>((v19.second)) /  /* avg_qty_count_for_mean */get<5>((v19.second))),( /* avg_price_sum_for_mean */get<6>((v19.second)) /  /* avg_price_count_for_mean */get<7>((v19.second))),( /* avg_disc_sum_for_mean */get<8>((v19.second)) /  /* avg_disc_count_for_mean */get<9>((v19.second))), /* count_order */get<10>((v19.second)))] += true;}const auto& results = v20;const auto& out = results;

    FastDict_s1s1ffffffff_b* result = (FastDict_s1s1ffffffff_b*)PyObject_CallObject(PyObject_CallObject(PyObject_GetAttrString(PyImport_Import(PyUnicode_FromString("test_unopt_fastdict_compiled")), (char*)"new_FastDict_s1s1ffffffff_b"), nullptr), nullptr);
    *(result->dict) = out;

    return (PyObject*)PyObject_CallObject(PyObject_GetAttrString(PyImport_Import(PyUnicode_FromString("sdqlpy.fastd")), "fastd"), Py_BuildValue("(OO)", result, PyUnicode_FromString("test_unopt")));

}

static PyObject * q2(PyObject *self, PyObject* args)
{
	

    PyObject * db_;
    if (!PyArg_ParseTuple(args, "O", &db_)){
        PyErr_SetString(PyExc_ValueError,"Error while parsing the sum inputs.");
        return NULL;
    }
    
    
    DB dbobj;
    DB* db = &dbobj;
    
const static int numpy_initialized =  init_numpy();


	auto pa_size = PyArray_Size(PyList_GetItem(PyList_GetItem(db_, 0), 0));
	db->pa_dataset_size = pa_size;
	auto p_partkey = (long*)PyArray_DATA(PyList_GetItem(PyList_GetItem(db_, 0), 0));
	auto p_name= (VarChar<55>*)PyArray_DATA(PyList_GetItem(PyList_GetItem(db_, 0), 1));
	auto p_mfgr= (VarChar<25>*)PyArray_DATA(PyList_GetItem(PyList_GetItem(db_, 0), 2));
	auto p_brand= (VarChar<10>*)PyArray_DATA(PyList_GetItem(PyList_GetItem(db_, 0), 3));
	auto p_type= (VarChar<25>*)PyArray_DATA(PyList_GetItem(PyList_GetItem(db_, 0), 4));
	auto p_size = (long*)PyArray_DATA(PyList_GetItem(PyList_GetItem(db_, 0), 5));
	auto p_container= (VarChar<10>*)PyArray_DATA(PyList_GetItem(PyList_GetItem(db_, 0), 6));
	auto p_retailprice = (double*)PyArray_DATA(PyList_GetItem(PyList_GetItem(db_, 0), 7));
	auto p_comment= (VarChar<23>*)PyArray_DATA(PyList_GetItem(PyList_GetItem(db_, 0), 8));
	auto p_NA= (VarChar<1>*)PyArray_DATA(PyList_GetItem(PyList_GetItem(db_, 0), 9));

	auto su_size = PyArray_Size(PyList_GetItem(PyList_GetItem(db_, 1), 0));
	db->su_dataset_size = su_size;
	auto s_suppkey = (long*)PyArray_DATA(PyList_GetItem(PyList_GetItem(db_, 1), 0));
	auto s_name= (VarChar<25>*)PyArray_DATA(PyList_GetItem(PyList_GetItem(db_, 1), 1));
	auto s_address= (VarChar<40>*)PyArray_DATA(PyList_GetItem(PyList_GetItem(db_, 1), 2));
	auto s_nationkey = (long*)PyArray_DATA(PyList_GetItem(PyList_GetItem(db_, 1), 3));
	auto s_phone= (VarChar<15>*)PyArray_DATA(PyList_GetItem(PyList_GetItem(db_, 1), 4));
	auto s_acctbal = (double*)PyArray_DATA(PyList_GetItem(PyList_GetItem(db_, 1), 5));
	auto s_comment= (VarChar<101>*)PyArray_DATA(PyList_GetItem(PyList_GetItem(db_, 1), 6));
	auto s_NA= (VarChar<1>*)PyArray_DATA(PyList_GetItem(PyList_GetItem(db_, 1), 7));

	auto ps_size = PyArray_Size(PyList_GetItem(PyList_GetItem(db_, 2), 0));
	db->ps_dataset_size = ps_size;
	auto ps_partkey = (long*)PyArray_DATA(PyList_GetItem(PyList_GetItem(db_, 2), 0));
	auto ps_suppkey = (long*)PyArray_DATA(PyList_GetItem(PyList_GetItem(db_, 2), 1));
	auto ps_availqty = (double*)PyArray_DATA(PyList_GetItem(PyList_GetItem(db_, 2), 2));
	auto ps_supplycost = (double*)PyArray_DATA(PyList_GetItem(PyList_GetItem(db_, 2), 3));
	auto ps_comment= (VarChar<199>*)PyArray_DATA(PyList_GetItem(PyList_GetItem(db_, 2), 4));
	auto ps_NA= (VarChar<1>*)PyArray_DATA(PyList_GetItem(PyList_GetItem(db_, 2), 5));

	auto na_size = PyArray_Size(PyList_GetItem(PyList_GetItem(db_, 3), 0));
	db->na_dataset_size = na_size;
	auto n_nationkey = (long*)PyArray_DATA(PyList_GetItem(PyList_GetItem(db_, 3), 0));
	auto n_name= (VarChar<25>*)PyArray_DATA(PyList_GetItem(PyList_GetItem(db_, 3), 1));
	auto n_regionkey = (long*)PyArray_DATA(PyList_GetItem(PyList_GetItem(db_, 3), 2));
	auto n_comment= (VarChar<152>*)PyArray_DATA(PyList_GetItem(PyList_GetItem(db_, 3), 3));
	auto n_NA= (VarChar<1>*)PyArray_DATA(PyList_GetItem(PyList_GetItem(db_, 3), 4));

	auto re_size = PyArray_Size(PyList_GetItem(PyList_GetItem(db_, 4), 0));
	db->re_dataset_size = re_size;
	auto r_regionkey = (long*)PyArray_DATA(PyList_GetItem(PyList_GetItem(db_, 4), 0));
	auto r_name= (VarChar<25>*)PyArray_DATA(PyList_GetItem(PyList_GetItem(db_, 4), 1));
	auto r_comment= (VarChar<152>*)PyArray_DATA(PyList_GetItem(PyList_GetItem(db_, 4), 2));
	auto r_NA= (VarChar<1>*)PyArray_DATA(PyList_GetItem(PyList_GetItem(db_, 4), 3));



	
auto v83 = phmap::flat_hash_map<tuple<long,VarChar<25>,VarChar<152>,VarChar<1>>,bool>({});
tbb::enumerable_thread_specific<phmap::flat_hash_map<tuple<long,VarChar<25>,VarChar<152>,VarChar<1>>,bool>> v140;
auto v85 = phmap::flat_hash_map<long,phmap::flat_hash_map<tuple<long,VarChar<25>,VarChar<152>,VarChar<1>>,bool>>({});
auto v89 = phmap::flat_hash_map<tuple<long,VarChar<25>,long,VarChar<152>,VarChar<1>,long,VarChar<25>,VarChar<152>,VarChar<1>>,bool>({});
tbb::enumerable_thread_specific<phmap::flat_hash_map<tuple<long,VarChar<25>,long,VarChar<152>,VarChar<1>,long,VarChar<25>,VarChar<152>,VarChar<1>>,bool>> v147;
auto v91 = phmap::flat_hash_map<long,phmap::flat_hash_map<tuple<long,VarChar<25>,long,VarChar<152>,VarChar<1>,long,VarChar<25>,VarChar<152>,VarChar<1>>,bool>>({});
auto v95 = phmap::flat_hash_map<tuple<long,VarChar<25>,VarChar<40>,long,VarChar<15>,double,VarChar<101>,VarChar<1>,long,VarChar<25>,long,VarChar<152>,VarChar<1>,long,VarChar<25>,VarChar<152>,VarChar<1>>,bool>({});
tbb::enumerable_thread_specific<phmap::flat_hash_map<tuple<long,VarChar<25>,VarChar<40>,long,VarChar<15>,double,VarChar<101>,VarChar<1>,long,VarChar<25>,long,VarChar<152>,VarChar<1>,long,VarChar<25>,VarChar<152>,VarChar<1>>,bool>> v154;
auto v99 = phmap::flat_hash_map<tuple<long,VarChar<25>,VarChar<40>,long,VarChar<15>,double,VarChar<101>,VarChar<1>,long,VarChar<25>,long,VarChar<152>,VarChar<1>,long,VarChar<25>,VarChar<152>,VarChar<1>>,bool>({});
tbb::enumerable_thread_specific<phmap::flat_hash_map<tuple<long,VarChar<25>,VarChar<40>,long,VarChar<15>,double,VarChar<101>,VarChar<1>,long,VarChar<25>,long,VarChar<152>,VarChar<1>,long,VarChar<25>,VarChar<152>,VarChar<1>>,bool>> v161;
auto v101 = phmap::flat_hash_map<long,phmap::flat_hash_map<tuple<long,VarChar<25>,VarChar<40>,long,VarChar<15>,double,VarChar<101>,VarChar<1>,long,VarChar<25>,long,VarChar<152>,VarChar<1>,long,VarChar<25>,VarChar<152>,VarChar<1>>,bool>>({});
auto v105 = phmap::flat_hash_map<tuple<long,long,double,double,VarChar<199>,VarChar<1>,long,VarChar<25>,VarChar<40>,long,VarChar<15>,double,VarChar<101>,VarChar<1>,long,VarChar<25>,long,VarChar<152>,VarChar<1>,long,VarChar<25>,VarChar<152>,VarChar<1>>,bool>({});
tbb::enumerable_thread_specific<phmap::flat_hash_map<tuple<long,long,double,double,VarChar<199>,VarChar<1>,long,VarChar<25>,VarChar<40>,long,VarChar<15>,double,VarChar<101>,VarChar<1>,long,VarChar<25>,long,VarChar<152>,VarChar<1>,long,VarChar<25>,VarChar<152>,VarChar<1>>,bool>> v168;
auto v107 = phmap::flat_hash_map<tuple<long>,tuple<double>>({});
auto v109 = phmap::flat_hash_map<tuple<long,double>,bool>({});
auto v111 = phmap::flat_hash_map<tuple<long,VarChar<55>,VarChar<25>,VarChar<10>,VarChar<25>,long,VarChar<10>,double,VarChar<23>,VarChar<1>>,bool>({});
tbb::enumerable_thread_specific<phmap::flat_hash_map<tuple<long,VarChar<55>,VarChar<25>,VarChar<10>,VarChar<25>,long,VarChar<10>,double,VarChar<23>,VarChar<1>>,bool>> v175;
auto v113 = phmap::flat_hash_map<long,phmap::flat_hash_map<tuple<long,VarChar<55>,VarChar<25>,VarChar<10>,VarChar<25>,long,VarChar<10>,double,VarChar<23>,VarChar<1>>,bool>>({});
auto v117 = phmap::flat_hash_map<tuple<long,long,double,double,VarChar<199>,VarChar<1>,long,VarChar<55>,VarChar<25>,VarChar<10>,VarChar<25>,long,VarChar<10>,double,VarChar<23>,VarChar<1>>,bool>({});
tbb::enumerable_thread_specific<phmap::flat_hash_map<tuple<long,long,double,double,VarChar<199>,VarChar<1>,long,VarChar<55>,VarChar<25>,VarChar<10>,VarChar<25>,long,VarChar<10>,double,VarChar<23>,VarChar<1>>,bool>> v182;
auto v119 = phmap::flat_hash_map<long,phmap::flat_hash_map<tuple<long,double>,bool>>({});
auto v123 = phmap::flat_hash_map<tuple<long,long,double,double,VarChar<199>,VarChar<1>,long,VarChar<55>,VarChar<25>,VarChar<10>,VarChar<25>,long,VarChar<10>,double,VarChar<23>,VarChar<1>,long,double>,bool>({});
auto v125 = phmap::flat_hash_map<long,phmap::flat_hash_map<tuple<long,VarChar<25>,VarChar<40>,long,VarChar<15>,double,VarChar<101>,VarChar<1>,long,VarChar<25>,long,VarChar<152>,VarChar<1>,long,VarChar<25>,VarChar<152>,VarChar<1>>,bool>>({});
auto v129 = phmap::flat_hash_map<tuple<long,long,double,double,VarChar<199>,VarChar<1>,long,VarChar<55>,VarChar<25>,VarChar<10>,VarChar<25>,long,VarChar<10>,double,VarChar<23>,VarChar<1>,long,double,long,VarChar<25>,VarChar<40>,long,VarChar<15>,double,VarChar<101>,VarChar<1>,long,VarChar<25>,long,VarChar<152>,VarChar<1>,long,VarChar<25>,VarChar<152>,VarChar<1>>,bool>({});
auto v131 = phmap::flat_hash_map<tuple<long,long,double,double,VarChar<199>,VarChar<1>,long,VarChar<55>,VarChar<25>,VarChar<10>,VarChar<25>,long,VarChar<10>,double,VarChar<23>,VarChar<1>,long,double,long,VarChar<25>,VarChar<40>,long,VarChar<15>,double,VarChar<101>,VarChar<1>,long,VarChar<25>,long,VarChar<152>,VarChar<1>,long,VarChar<25>,VarChar<152>,VarChar<1>>,bool>({});
auto v133 = phmap::flat_hash_map<tuple<long,long,double,double,VarChar<199>,VarChar<1>,long,VarChar<55>,VarChar<25>,VarChar<10>,VarChar<25>,long,VarChar<10>,double,VarChar<23>,VarChar<1>,long,double,long,VarChar<25>,VarChar<40>,long,VarChar<15>,double,VarChar<101>,VarChar<1>,long,VarChar<25>,long,VarChar<152>,VarChar<1>,long,VarChar<25>,VarChar<152>,VarChar<1>>,phmap::flat_hash_map<tuple<double,VarChar<25>,VarChar<25>,long,VarChar<25>,VarChar<40>,VarChar<15>,VarChar<101>>,bool>>({});
auto v135 = phmap::flat_hash_map<tuple<double,VarChar<25>,VarChar<25>,long,VarChar<25>,VarChar<40>,VarChar<15>,VarChar<101>>,bool>({});	
	const auto& europe = ConstantString("EUROPE", 7);const auto& brass = ConstantString("BRASS", 6);

auto v136 = db->re_dataset_size; tbb::parallel_for(tbb::blocked_range<size_t>(0, v136), [&](const tbb::blocked_range<size_t>& v137){auto& v141=v140.local();for (size_t v82=v137.begin(), end=v137.end(); v82!=end; ++v82){if ((r_name[v82] == europe)){v141[make_tuple(r_regionkey[v82], r_name[v82], r_comment[v82], r_NA[v82])] += true;};}});for (auto& local : v140)AddMap<phmap::flat_hash_map<tuple<long,VarChar<25>,VarChar<152>,VarChar<1>>,bool>,tuple<long,VarChar<25>,VarChar<152>,VarChar<1>>,bool>(v83, local);const auto& region_nation_build_pre_ops = v83;

for (auto& v84 : region_nation_build_pre_ops){v85[ /* r_regionkey */get<0>((v84.first))] += phmap::flat_hash_map<tuple<long,VarChar<25>,VarChar<152>,VarChar<1>>,bool>({{(v84.first), (v84.second)}});}const auto& region_nation_build_nest_dict = v85;

auto v143 = db->na_dataset_size; tbb::parallel_for(tbb::blocked_range<size_t>(0, v143), [&](const tbb::blocked_range<size_t>& v144){auto& v148=v147.local();for (size_t v86=v144.begin(), end=v144.end(); v86!=end; ++v86){if (((region_nation_build_nest_dict).contains(n_regionkey[v86]))){for (auto& v87 : (region_nation_build_nest_dict).at(n_regionkey[v86])){v148[tuple_cat(make_tuple(n_nationkey[v86], n_name[v86], n_regionkey[v86], n_comment[v86], n_NA[v86]),move(make_tuple(r_regionkey[v87], r_name[v87], r_comment[v87], r_NA[v87])))] += true;};};}});for (auto& local : v147)AddMap<phmap::flat_hash_map<tuple<long,VarChar<25>,long,VarChar<152>,VarChar<1>,long,VarChar<25>,VarChar<152>,VarChar<1>>,bool>,tuple<long,VarChar<25>,long,VarChar<152>,VarChar<1>,long,VarChar<25>,VarChar<152>,VarChar<1>>,bool>(v89, local);const auto& region_nation_supplier_build_pre_ops = v89;

for (auto& v90 : region_nation_supplier_build_pre_ops){v91[ /* n_nationkey */get<0>((v90.first))] += phmap::flat_hash_map<tuple<long,VarChar<25>,long,VarChar<152>,VarChar<1>,long,VarChar<25>,VarChar<152>,VarChar<1>>,bool>({{(v90.first), (v90.second)}});}const auto& region_nation_supplier_build_nest_dict = v91;

auto v150 = db->su_dataset_size; tbb::parallel_for(tbb::blocked_range<size_t>(0, v150), [&](const tbb::blocked_range<size_t>& v151){auto& v155=v154.local();for (size_t v92=v151.begin(), end=v151.end(); v92!=end; ++v92){if (((region_nation_supplier_build_nest_dict).contains(s_nationkey[v92]))){for (auto& v93 : (region_nation_supplier_build_nest_dict).at(s_nationkey[v92])){v155[tuple_cat(make_tuple(s_suppkey[v92], s_name[v92], s_address[v92], s_nationkey[v92], s_phone[v92], s_acctbal[v92], s_comment[v92], s_NA[v92]),move(make_tuple(n_nationkey[v93], n_name[v93], n_regionkey[v93], n_comment[v93], n_NA[v93], r_regionkey[v93], r_name[v93], r_comment[v93], r_NA[v93])))] += true;};};}});for (auto& local : v154)AddMap<phmap::flat_hash_map<tuple<long,VarChar<25>,VarChar<40>,long,VarChar<15>,double,VarChar<101>,VarChar<1>,long,VarChar<25>,long,VarChar<152>,VarChar<1>,long,VarChar<25>,VarChar<152>,VarChar<1>>,bool>,tuple<long,VarChar<25>,VarChar<40>,long,VarChar<15>,double,VarChar<101>,VarChar<1>,long,VarChar<25>,long,VarChar<152>,VarChar<1>,long,VarChar<25>,VarChar<152>,VarChar<1>>,bool>(v95, local);const auto& region_nation_supplier_region_nation_supplier_ps1_part_partsupp_build_pre_ops = v95;

auto v157 = db->su_dataset_size; tbb::parallel_for(tbb::blocked_range<size_t>(0, v157), [&](const tbb::blocked_range<size_t>& v158){auto& v162=v161.local();for (size_t v96=v158.begin(), end=v158.end(); v96!=end; ++v96){if (((region_nation_supplier_build_nest_dict).contains(s_nationkey[v96]))){for (auto& v97 : (region_nation_supplier_build_nest_dict).at(s_nationkey[v96])){v162[tuple_cat(make_tuple(s_suppkey[v96], s_name[v96], s_address[v96], s_nationkey[v96], s_phone[v96], s_acctbal[v96], s_comment[v96], s_NA[v96]),move(make_tuple(n_nationkey[v97], n_name[v97], n_regionkey[v97], n_comment[v97], n_NA[v97], r_regionkey[v97], r_name[v97], r_comment[v97], r_NA[v97])))] += true;};};}});for (auto& local : v161)AddMap<phmap::flat_hash_map<tuple<long,VarChar<25>,VarChar<40>,long,VarChar<15>,double,VarChar<101>,VarChar<1>,long,VarChar<25>,long,VarChar<152>,VarChar<1>,long,VarChar<25>,VarChar<152>,VarChar<1>>,bool>,tuple<long,VarChar<25>,VarChar<40>,long,VarChar<15>,double,VarChar<101>,VarChar<1>,long,VarChar<25>,long,VarChar<152>,VarChar<1>,long,VarChar<25>,VarChar<152>,VarChar<1>>,bool>(v99, local);const auto& region_nation_supplier_ps1_build_pre_ops = v99;

for (auto& v100 : region_nation_supplier_ps1_build_pre_ops){v101[ /* s_suppkey */get<0>((v100.first))] += phmap::flat_hash_map<tuple<long,VarChar<25>,VarChar<40>,long,VarChar<15>,double,VarChar<101>,VarChar<1>,long,VarChar<25>,long,VarChar<152>,VarChar<1>,long,VarChar<25>,VarChar<152>,VarChar<1>>,bool>({{(v100.first), (v100.second)}});}const auto& region_nation_supplier_ps1_build_nest_dict = v101;

auto v164 = db->ps_dataset_size; tbb::parallel_for(tbb::blocked_range<size_t>(0, v164), [&](const tbb::blocked_range<size_t>& v165){auto& v169=v168.local();for (size_t v102=v165.begin(), end=v165.end(); v102!=end; ++v102){if (((region_nation_supplier_ps1_build_nest_dict).contains(ps_suppkey[v102]))){for (auto& v103 : (region_nation_supplier_ps1_build_nest_dict).at(ps_suppkey[v102])){v169[tuple_cat(make_tuple(ps_partkey[v102], ps_suppkey[v102], ps_availqty[v102], ps_supplycost[v102], ps_comment[v102], ps_NA[v102]),move(make_tuple(s_suppkey[v103], s_name[v103], s_address[v103], s_nationkey[v103], s_phone[v103], s_acctbal[v103], s_comment[v103], s_NA[v103], n_nationkey[v103], n_name[v103], n_regionkey[v103], n_comment[v103], n_NA[v103], r_regionkey[v103], r_name[v103], r_comment[v103], r_NA[v103])))] += true;};};}});for (auto& local : v168)AddMap<phmap::flat_hash_map<tuple<long,long,double,double,VarChar<199>,VarChar<1>,long,VarChar<25>,VarChar<40>,long,VarChar<15>,double,VarChar<101>,VarChar<1>,long,VarChar<25>,long,VarChar<152>,VarChar<1>,long,VarChar<25>,VarChar<152>,VarChar<1>>,bool>,tuple<long,long,double,double,VarChar<199>,VarChar<1>,long,VarChar<25>,VarChar<40>,long,VarChar<15>,double,VarChar<101>,VarChar<1>,long,VarChar<25>,long,VarChar<152>,VarChar<1>,long,VarChar<25>,VarChar<152>,VarChar<1>>,bool>(v105, local);const auto& region_nation_supplier_ps1_0 = v105;

for (auto& v106 : region_nation_supplier_ps1_0){v107[make_tuple( /* ps_partkey */get<0>((v106.first)))] += make_tuple( /* ps_supplycost */get<3>((v106.first)));}const auto& region_nation_supplier_ps1_1 = v107;

for (auto& v108 : region_nation_supplier_ps1_1){v109[tuple_cat((v108.first),move((v108.second)))] += true;}const auto& region_nation_supplier_ps1_part_partsupp_build_pre_ops = v109;

auto v171 = db->pa_dataset_size; tbb::parallel_for(tbb::blocked_range<size_t>(0, v171), [&](const tbb::blocked_range<size_t>& v172){auto& v176=v175.local();for (size_t v110=v172.begin(), end=v172.end(); v110!=end; ++v110){if (((p_type[v110].endsWith(brass)) && (p_size[v110] == (long)15))){v176[make_tuple(p_partkey[v110], p_name[v110], p_mfgr[v110], p_brand[v110], p_type[v110], p_size[v110], p_container[v110], p_retailprice[v110], p_comment[v110], p_NA[v110])] += true;};}});for (auto& local : v175)AddMap<phmap::flat_hash_map<tuple<long,VarChar<55>,VarChar<25>,VarChar<10>,VarChar<25>,long,VarChar<10>,double,VarChar<23>,VarChar<1>>,bool>,tuple<long,VarChar<55>,VarChar<25>,VarChar<10>,VarChar<25>,long,VarChar<10>,double,VarChar<23>,VarChar<1>>,bool>(v111, local);const auto& part_partsupp_build_pre_ops = v111;

for (auto& v112 : part_partsupp_build_pre_ops){v113[ /* p_partkey */get<0>((v112.first))] += phmap::flat_hash_map<tuple<long,VarChar<55>,VarChar<25>,VarChar<10>,VarChar<25>,long,VarChar<10>,double,VarChar<23>,VarChar<1>>,bool>({{(v112.first), (v112.second)}});}const auto& part_partsupp_build_nest_dict = v113;

auto v178 = db->ps_dataset_size; tbb::parallel_for(tbb::blocked_range<size_t>(0, v178), [&](const tbb::blocked_range<size_t>& v179){auto& v183=v182.local();for (size_t v114=v179.begin(), end=v179.end(); v114!=end; ++v114){if (((part_partsupp_build_nest_dict).contains(ps_partkey[v114]))){for (auto& v115 : (part_partsupp_build_nest_dict).at(ps_partkey[v114])){v183[tuple_cat(make_tuple(ps_partkey[v114], ps_suppkey[v114], ps_availqty[v114], ps_supplycost[v114], ps_comment[v114], ps_NA[v114]),move(make_tuple(p_partkey[v115], p_name[v115], p_mfgr[v115], p_brand[v115], p_type[v115], p_size[v115], p_container[v115], p_retailprice[v115], p_comment[v115], p_NA[v115])))] += true;};};}});for (auto& local : v182)AddMap<phmap::flat_hash_map<tuple<long,long,double,double,VarChar<199>,VarChar<1>,long,VarChar<55>,VarChar<25>,VarChar<10>,VarChar<25>,long,VarChar<10>,double,VarChar<23>,VarChar<1>>,bool>,tuple<long,long,double,double,VarChar<199>,VarChar<1>,long,VarChar<55>,VarChar<25>,VarChar<10>,VarChar<25>,long,VarChar<10>,double,VarChar<23>,VarChar<1>>,bool>(v117, local);const auto& region_nation_supplier_ps1_part_partsupp_probe_pre_ops = v117;

for (auto& v118 : region_nation_supplier_ps1_part_partsupp_build_pre_ops){v119[ /* ps_partkey */get<0>((v118.first))] += phmap::flat_hash_map<tuple<long,double>,bool>({{(v118.first), (v118.second)}});}const auto& region_nation_supplier_ps1_part_partsupp_build_nest_dict = v119;

for (auto& v120 : region_nation_supplier_ps1_part_partsupp_probe_pre_ops){if (((region_nation_supplier_ps1_part_partsupp_build_nest_dict).contains( /* ps_partkey */get<0>((v120.first))))){for (auto& v121 : (region_nation_supplier_ps1_part_partsupp_build_nest_dict).at( /* ps_partkey */get<0>((v120.first)))){v123[tuple_cat((v120.first),move((v121.first)))] += true;};};}const auto& region_nation_supplier_region_nation_supplier_ps1_part_partsupp_probe_pre_ops = v123;

for (auto& v124 : region_nation_supplier_region_nation_supplier_ps1_part_partsupp_build_pre_ops){v125[ /* s_suppkey */get<0>((v124.first))] += phmap::flat_hash_map<tuple<long,VarChar<25>,VarChar<40>,long,VarChar<15>,double,VarChar<101>,VarChar<1>,long,VarChar<25>,long,VarChar<152>,VarChar<1>,long,VarChar<25>,VarChar<152>,VarChar<1>>,bool>({{(v124.first), (v124.second)}});}const auto& region_nation_supplier_region_nation_supplier_ps1_part_partsupp_build_nest_dict = v125;

for (auto& v126 : region_nation_supplier_region_nation_supplier_ps1_part_partsupp_probe_pre_ops){if (((region_nation_supplier_region_nation_supplier_ps1_part_partsupp_build_nest_dict).contains( /* ps_suppkey */get<1>((v126.first))))){for (auto& v127 : (region_nation_supplier_region_nation_supplier_ps1_part_partsupp_build_nest_dict).at( /* ps_suppkey */get<1>((v126.first)))){v129[tuple_cat((v126.first),move((v127.first)))] += true;};};}const auto& region_nation_supplier_region_nation_supplier_ps1_part_partsupp_0 = v129;

for (auto& v130 : region_nation_supplier_region_nation_supplier_ps1_part_partsupp_0){if (( /* ps_supplycost */get<3>((v130.first)) ==  /* min_supplycost */get<17>((v130.first)))){v131[(v130.first)] += (v130.second);};}const auto& region_nation_supplier_region_nation_supplier_ps1_part_partsupp_1 = v131;

for (auto& v132 : region_nation_supplier_region_nation_supplier_ps1_part_partsupp_1){v133[(v132.first)] += phmap::flat_hash_map<tuple<double,VarChar<25>,VarChar<25>,long,VarChar<25>,VarChar<40>,VarChar<15>,VarChar<101>>,bool>({{make_tuple( /* s_acctbal */get<23>((v132.first)), /* s_name */get<19>((v132.first)), /* n_name */get<27>((v132.first)), /* p_partkey */get<6>((v132.first)), /* p_mfgr */get<8>((v132.first)), /* s_address */get<20>((v132.first)), /* s_phone */get<22>((v132.first)), /* s_comment */get<24>((v132.first))), true}});}const auto& region_nation_supplier_region_nation_supplier_ps1_part_partsupp_2 = v133;

for (auto& v134 : region_nation_supplier_region_nation_supplier_ps1_part_partsupp_2){(v134.second);}const auto& results = v135;const auto& out = results;

    FastDict_fs25s25is25s40s15s101_b* result = (FastDict_fs25s25is25s40s15s101_b*)PyObject_CallObject(PyObject_CallObject(PyObject_GetAttrString(PyImport_Import(PyUnicode_FromString("test_unopt_fastdict_compiled")), (char*)"new_FastDict_fs25s25is25s40s15s101_b"), nullptr), nullptr);
    *(result->dict) = out;

    return (PyObject*)PyObject_CallObject(PyObject_GetAttrString(PyImport_Import(PyUnicode_FromString("sdqlpy.fastd")), "fastd"), Py_BuildValue("(OO)", result, PyUnicode_FromString("test_unopt")));

}

static PyObject * q3(PyObject *self, PyObject* args)
{
	

    PyObject * db_;
    if (!PyArg_ParseTuple(args, "O", &db_)){
        PyErr_SetString(PyExc_ValueError,"Error while parsing the sum inputs.");
        return NULL;
    }
    
    
    DB dbobj;
    DB* db = &dbobj;
    
const static int numpy_initialized =  init_numpy();


	auto li_size = PyArray_Size(PyList_GetItem(PyList_GetItem(db_, 0), 0));
	db->li_dataset_size = li_size;
	auto l_orderkey = (long*)PyArray_DATA(PyList_GetItem(PyList_GetItem(db_, 0), 0));
	auto l_partkey = (long*)PyArray_DATA(PyList_GetItem(PyList_GetItem(db_, 0), 1));
	auto l_suppkey = (long*)PyArray_DATA(PyList_GetItem(PyList_GetItem(db_, 0), 2));
	auto l_linenumber = (long*)PyArray_DATA(PyList_GetItem(PyList_GetItem(db_, 0), 3));
	auto l_quantity = (double*)PyArray_DATA(PyList_GetItem(PyList_GetItem(db_, 0), 4));
	auto l_extendedprice = (double*)PyArray_DATA(PyList_GetItem(PyList_GetItem(db_, 0), 5));
	auto l_discount = (double*)PyArray_DATA(PyList_GetItem(PyList_GetItem(db_, 0), 6));
	auto l_tax = (double*)PyArray_DATA(PyList_GetItem(PyList_GetItem(db_, 0), 7));
	auto l_returnflag= (VarChar<1>*)PyArray_DATA(PyList_GetItem(PyList_GetItem(db_, 0), 8));
	auto l_linestatus= (VarChar<1>*)PyArray_DATA(PyList_GetItem(PyList_GetItem(db_, 0), 9));
	auto l_shipdate = (long*)PyArray_DATA(PyList_GetItem(PyList_GetItem(db_, 0), 10));
	auto l_commitdate = (long*)PyArray_DATA(PyList_GetItem(PyList_GetItem(db_, 0), 11));
	auto l_receiptdate = (long*)PyArray_DATA(PyList_GetItem(PyList_GetItem(db_, 0), 12));
	auto l_shipinstruct= (VarChar<25>*)PyArray_DATA(PyList_GetItem(PyList_GetItem(db_, 0), 13));
	auto l_shipmode= (VarChar<10>*)PyArray_DATA(PyList_GetItem(PyList_GetItem(db_, 0), 14));
	auto l_comment= (VarChar<44>*)PyArray_DATA(PyList_GetItem(PyList_GetItem(db_, 0), 15));
	auto l_NA= (VarChar<1>*)PyArray_DATA(PyList_GetItem(PyList_GetItem(db_, 0), 16));

	auto cu_size = PyArray_Size(PyList_GetItem(PyList_GetItem(db_, 1), 0));
	db->cu_dataset_size = cu_size;
	auto c_custkey = (long*)PyArray_DATA(PyList_GetItem(PyList_GetItem(db_, 1), 0));
	auto c_name= (VarChar<25>*)PyArray_DATA(PyList_GetItem(PyList_GetItem(db_, 1), 1));
	auto c_address= (VarChar<40>*)PyArray_DATA(PyList_GetItem(PyList_GetItem(db_, 1), 2));
	auto c_nationkey = (long*)PyArray_DATA(PyList_GetItem(PyList_GetItem(db_, 1), 3));
	auto c_phone= (VarChar<15>*)PyArray_DATA(PyList_GetItem(PyList_GetItem(db_, 1), 4));
	auto c_acctbal = (double*)PyArray_DATA(PyList_GetItem(PyList_GetItem(db_, 1), 5));
	auto c_mktsegment= (VarChar<10>*)PyArray_DATA(PyList_GetItem(PyList_GetItem(db_, 1), 6));
	auto c_comment= (VarChar<117>*)PyArray_DATA(PyList_GetItem(PyList_GetItem(db_, 1), 7));
	auto c_NA= (VarChar<1>*)PyArray_DATA(PyList_GetItem(PyList_GetItem(db_, 1), 8));

	auto ord_size = PyArray_Size(PyList_GetItem(PyList_GetItem(db_, 2), 0));
	db->ord_dataset_size = ord_size;
	auto o_orderkey = (long*)PyArray_DATA(PyList_GetItem(PyList_GetItem(db_, 2), 0));
	auto o_custkey = (long*)PyArray_DATA(PyList_GetItem(PyList_GetItem(db_, 2), 1));
	auto o_orderstatus= (VarChar<1>*)PyArray_DATA(PyList_GetItem(PyList_GetItem(db_, 2), 2));
	auto o_totalprice = (double*)PyArray_DATA(PyList_GetItem(PyList_GetItem(db_, 2), 3));
	auto o_orderdate = (long*)PyArray_DATA(PyList_GetItem(PyList_GetItem(db_, 2), 4));
	auto o_orderpriority= (VarChar<15>*)PyArray_DATA(PyList_GetItem(PyList_GetItem(db_, 2), 5));
	auto o_clerk= (VarChar<15>*)PyArray_DATA(PyList_GetItem(PyList_GetItem(db_, 2), 6));
	auto o_shippriority = (long*)PyArray_DATA(PyList_GetItem(PyList_GetItem(db_, 2), 7));
	auto o_comment= (VarChar<79>*)PyArray_DATA(PyList_GetItem(PyList_GetItem(db_, 2), 8));
	auto o_NA= (VarChar<1>*)PyArray_DATA(PyList_GetItem(PyList_GetItem(db_, 2), 9));



	
auto v210 = phmap::flat_hash_map<tuple<long,VarChar<25>,VarChar<40>,long,VarChar<15>,double,VarChar<10>,VarChar<117>,VarChar<1>>,bool>({});
tbb::enumerable_thread_specific<phmap::flat_hash_map<tuple<long,VarChar<25>,VarChar<40>,long,VarChar<15>,double,VarChar<10>,VarChar<117>,VarChar<1>>,bool>> v237;
auto v212 = phmap::flat_hash_map<tuple<long,long,VarChar<1>,double,long,VarChar<15>,VarChar<15>,long,VarChar<79>,VarChar<1>>,bool>({});
tbb::enumerable_thread_specific<phmap::flat_hash_map<tuple<long,long,VarChar<1>,double,long,VarChar<15>,VarChar<15>,long,VarChar<79>,VarChar<1>>,bool>> v244;
auto v214 = phmap::flat_hash_map<long,phmap::flat_hash_map<tuple<long,VarChar<25>,VarChar<40>,long,VarChar<15>,double,VarChar<10>,VarChar<117>,VarChar<1>>,bool>>({});
auto v218 = phmap::flat_hash_map<tuple<long,long,VarChar<1>,double,long,VarChar<15>,VarChar<15>,long,VarChar<79>,VarChar<1>,long,VarChar<25>,VarChar<40>,long,VarChar<15>,double,VarChar<10>,VarChar<117>,VarChar<1>>,bool>({});
auto v220 = phmap::flat_hash_map<tuple<long,long,long,long,double,double,double,double,VarChar<1>,VarChar<1>,long,long,long,VarChar<25>,VarChar<10>,VarChar<44>,VarChar<1>>,bool>({});
tbb::enumerable_thread_specific<phmap::flat_hash_map<tuple<long,long,long,long,double,double,double,double,VarChar<1>,VarChar<1>,long,long,long,VarChar<25>,VarChar<10>,VarChar<44>,VarChar<1>>,bool>> v251;
auto v222 = phmap::flat_hash_map<long,phmap::flat_hash_map<tuple<long,long,VarChar<1>,double,long,VarChar<15>,VarChar<15>,long,VarChar<79>,VarChar<1>,long,VarChar<25>,VarChar<40>,long,VarChar<15>,double,VarChar<10>,VarChar<117>,VarChar<1>>,bool>>({});
auto v226 = phmap::flat_hash_map<tuple<long,long,long,long,double,double,double,double,VarChar<1>,VarChar<1>,long,long,long,VarChar<25>,VarChar<10>,VarChar<44>,VarChar<1>,long,long,VarChar<1>,double,long,VarChar<15>,VarChar<15>,long,VarChar<79>,VarChar<1>,long,VarChar<25>,VarChar<40>,long,VarChar<15>,double,VarChar<10>,VarChar<117>,VarChar<1>>,bool>({});
auto v228 = phmap::flat_hash_map<tuple<long,long,long,long,double,double,double,double,VarChar<1>,VarChar<1>,long,long,long,VarChar<25>,VarChar<10>,VarChar<44>,VarChar<1>,long,long,VarChar<1>,double,long,VarChar<15>,VarChar<15>,long,VarChar<79>,VarChar<1>,long,VarChar<25>,VarChar<40>,long,VarChar<15>,double,VarChar<10>,VarChar<117>,VarChar<1>,double>,bool>({});
auto v230 = phmap::flat_hash_map<tuple<long,long,long>,tuple<double>>({});
auto v232 = phmap::flat_hash_map<tuple<long,long,long,double>,bool>({});	
	const auto& building = ConstantString("BUILDING", 9);

auto v233 = db->cu_dataset_size; tbb::parallel_for(tbb::blocked_range<size_t>(0, v233), [&](const tbb::blocked_range<size_t>& v234){auto& v238=v237.local();for (size_t v209=v234.begin(), end=v234.end(); v209!=end; ++v209){if ((c_mktsegment[v209] == building)){v238[make_tuple(c_custkey[v209], c_name[v209], c_address[v209], c_nationkey[v209], c_phone[v209], c_acctbal[v209], c_mktsegment[v209], c_comment[v209], c_NA[v209])] += true;};}});for (auto& local : v237)AddMap<phmap::flat_hash_map<tuple<long,VarChar<25>,VarChar<40>,long,VarChar<15>,double,VarChar<10>,VarChar<117>,VarChar<1>>,bool>,tuple<long,VarChar<25>,VarChar<40>,long,VarChar<15>,double,VarChar<10>,VarChar<117>,VarChar<1>>,bool>(v210, local);const auto& customer_orders_build_pre_ops = v210;

auto v240 = db->ord_dataset_size; tbb::parallel_for(tbb::blocked_range<size_t>(0, v240), [&](const tbb::blocked_range<size_t>& v241){auto& v245=v244.local();for (size_t v211=v241.begin(), end=v241.end(); v211!=end; ++v211){if ((o_orderdate[v211] < (long)19950315)){v245[make_tuple(o_orderkey[v211], o_custkey[v211], o_orderstatus[v211], o_totalprice[v211], o_orderdate[v211], o_orderpriority[v211], o_clerk[v211], o_shippriority[v211], o_comment[v211], o_NA[v211])] += true;};}});for (auto& local : v244)AddMap<phmap::flat_hash_map<tuple<long,long,VarChar<1>,double,long,VarChar<15>,VarChar<15>,long,VarChar<79>,VarChar<1>>,bool>,tuple<long,long,VarChar<1>,double,long,VarChar<15>,VarChar<15>,long,VarChar<79>,VarChar<1>>,bool>(v212, local);const auto& customer_orders_probe_pre_ops = v212;

for (auto& v213 : customer_orders_build_pre_ops){v214[ /* c_custkey */get<0>((v213.first))] += phmap::flat_hash_map<tuple<long,VarChar<25>,VarChar<40>,long,VarChar<15>,double,VarChar<10>,VarChar<117>,VarChar<1>>,bool>({{(v213.first), (v213.second)}});}const auto& customer_orders_build_nest_dict = v214;

for (auto& v215 : customer_orders_probe_pre_ops){if (((customer_orders_build_nest_dict).contains( /* o_custkey */get<1>((v215.first))))){for (auto& v216 : (customer_orders_build_nest_dict).at( /* o_custkey */get<1>((v215.first)))){v218[tuple_cat((v215.first),move((v216.first)))] += true;};};}const auto& customer_orders_lineitem_build_pre_ops = v218;

auto v247 = db->li_dataset_size; tbb::parallel_for(tbb::blocked_range<size_t>(0, v247), [&](const tbb::blocked_range<size_t>& v248){auto& v252=v251.local();for (size_t v219=v248.begin(), end=v248.end(); v219!=end; ++v219){if ((l_shipdate[v219] > (long)19950315)){v252[make_tuple(l_orderkey[v219], l_partkey[v219], l_suppkey[v219], l_linenumber[v219], l_quantity[v219], l_extendedprice[v219], l_discount[v219], l_tax[v219], l_returnflag[v219], l_linestatus[v219], l_shipdate[v219], l_commitdate[v219], l_receiptdate[v219], l_shipinstruct[v219], l_shipmode[v219], l_comment[v219], l_NA[v219])] += true;};}});for (auto& local : v251)AddMap<phmap::flat_hash_map<tuple<long,long,long,long,double,double,double,double,VarChar<1>,VarChar<1>,long,long,long,VarChar<25>,VarChar<10>,VarChar<44>,VarChar<1>>,bool>,tuple<long,long,long,long,double,double,double,double,VarChar<1>,VarChar<1>,long,long,long,VarChar<25>,VarChar<10>,VarChar<44>,VarChar<1>>,bool>(v220, local);const auto& customer_orders_lineitem_probe_pre_ops = v220;

for (auto& v221 : customer_orders_lineitem_build_pre_ops){v222[ /* o_orderkey */get<0>((v221.first))] += phmap::flat_hash_map<tuple<long,long,VarChar<1>,double,long,VarChar<15>,VarChar<15>,long,VarChar<79>,VarChar<1>,long,VarChar<25>,VarChar<40>,long,VarChar<15>,double,VarChar<10>,VarChar<117>,VarChar<1>>,bool>({{(v221.first), (v221.second)}});}const auto& customer_orders_lineitem_build_nest_dict = v222;

for (auto& v223 : customer_orders_lineitem_probe_pre_ops){if (((customer_orders_lineitem_build_nest_dict).contains( /* l_orderkey */get<0>((v223.first))))){for (auto& v224 : (customer_orders_lineitem_build_nest_dict).at( /* l_orderkey */get<0>((v223.first)))){v226[tuple_cat((v223.first),move((v224.first)))] += true;};};}const auto& customer_orders_lineitem_0 = v226;

for (auto& v227 : customer_orders_lineitem_0){v228[tuple_cat((v227.first),move(make_tuple(( /* l_extendedprice */get<5>((v227.first)) * (1.0 -  /* l_discount */get<6>((v227.first)))))))] += (v227.second);}const auto& customer_orders_lineitem_1 = v228;

for (auto& v229 : customer_orders_lineitem_1){v230[make_tuple( /* l_orderkey */get<0>((v229.first)), /* o_orderdate */get<21>((v229.first)), /* o_shippriority */get<24>((v229.first)))] += make_tuple( /* revenue */get<36>((v229.first)));}const auto& customer_orders_lineitem_2 = v230;

for (auto& v231 : customer_orders_lineitem_2){v232[tuple_cat((v231.first),move((v231.second)))] += true;}const auto& results = v232;const auto& out = results;

    FastDict_iiif_b* result = (FastDict_iiif_b*)PyObject_CallObject(PyObject_CallObject(PyObject_GetAttrString(PyImport_Import(PyUnicode_FromString("test_unopt_fastdict_compiled")), (char*)"new_FastDict_iiif_b"), nullptr), nullptr);
    *(result->dict) = out;

    return (PyObject*)PyObject_CallObject(PyObject_GetAttrString(PyImport_Import(PyUnicode_FromString("sdqlpy.fastd")), "fastd"), Py_BuildValue("(OO)", result, PyUnicode_FromString("test_unopt")));

}

static PyObject * q4(PyObject *self, PyObject* args)
{
	

    PyObject * db_;
    if (!PyArg_ParseTuple(args, "O", &db_)){
        PyErr_SetString(PyExc_ValueError,"Error while parsing the sum inputs.");
        return NULL;
    }
    
    
    DB dbobj;
    DB* db = &dbobj;
    
const static int numpy_initialized =  init_numpy();


	auto ord_size = PyArray_Size(PyList_GetItem(PyList_GetItem(db_, 0), 0));
	db->ord_dataset_size = ord_size;
	auto o_orderkey = (long*)PyArray_DATA(PyList_GetItem(PyList_GetItem(db_, 0), 0));
	auto o_custkey = (long*)PyArray_DATA(PyList_GetItem(PyList_GetItem(db_, 0), 1));
	auto o_orderstatus= (VarChar<1>*)PyArray_DATA(PyList_GetItem(PyList_GetItem(db_, 0), 2));
	auto o_totalprice = (double*)PyArray_DATA(PyList_GetItem(PyList_GetItem(db_, 0), 3));
	auto o_orderdate = (long*)PyArray_DATA(PyList_GetItem(PyList_GetItem(db_, 0), 4));
	auto o_orderpriority= (VarChar<15>*)PyArray_DATA(PyList_GetItem(PyList_GetItem(db_, 0), 5));
	auto o_clerk= (VarChar<15>*)PyArray_DATA(PyList_GetItem(PyList_GetItem(db_, 0), 6));
	auto o_shippriority = (long*)PyArray_DATA(PyList_GetItem(PyList_GetItem(db_, 0), 7));
	auto o_comment= (VarChar<79>*)PyArray_DATA(PyList_GetItem(PyList_GetItem(db_, 0), 8));
	auto o_NA= (VarChar<1>*)PyArray_DATA(PyList_GetItem(PyList_GetItem(db_, 0), 9));

	auto li_size = PyArray_Size(PyList_GetItem(PyList_GetItem(db_, 1), 0));
	db->li_dataset_size = li_size;
	auto l_orderkey = (long*)PyArray_DATA(PyList_GetItem(PyList_GetItem(db_, 1), 0));
	auto l_partkey = (long*)PyArray_DATA(PyList_GetItem(PyList_GetItem(db_, 1), 1));
	auto l_suppkey = (long*)PyArray_DATA(PyList_GetItem(PyList_GetItem(db_, 1), 2));
	auto l_linenumber = (long*)PyArray_DATA(PyList_GetItem(PyList_GetItem(db_, 1), 3));
	auto l_quantity = (double*)PyArray_DATA(PyList_GetItem(PyList_GetItem(db_, 1), 4));
	auto l_extendedprice = (double*)PyArray_DATA(PyList_GetItem(PyList_GetItem(db_, 1), 5));
	auto l_discount = (double*)PyArray_DATA(PyList_GetItem(PyList_GetItem(db_, 1), 6));
	auto l_tax = (double*)PyArray_DATA(PyList_GetItem(PyList_GetItem(db_, 1), 7));
	auto l_returnflag= (VarChar<1>*)PyArray_DATA(PyList_GetItem(PyList_GetItem(db_, 1), 8));
	auto l_linestatus= (VarChar<1>*)PyArray_DATA(PyList_GetItem(PyList_GetItem(db_, 1), 9));
	auto l_shipdate = (long*)PyArray_DATA(PyList_GetItem(PyList_GetItem(db_, 1), 10));
	auto l_commitdate = (long*)PyArray_DATA(PyList_GetItem(PyList_GetItem(db_, 1), 11));
	auto l_receiptdate = (long*)PyArray_DATA(PyList_GetItem(PyList_GetItem(db_, 1), 12));
	auto l_shipinstruct= (VarChar<25>*)PyArray_DATA(PyList_GetItem(PyList_GetItem(db_, 1), 13));
	auto l_shipmode= (VarChar<10>*)PyArray_DATA(PyList_GetItem(PyList_GetItem(db_, 1), 14));
	auto l_comment= (VarChar<44>*)PyArray_DATA(PyList_GetItem(PyList_GetItem(db_, 1), 15));
	auto l_NA= (VarChar<1>*)PyArray_DATA(PyList_GetItem(PyList_GetItem(db_, 1), 16));



	
auto v267 = phmap::flat_hash_map<tuple<long,long,long,long,double,double,double,double,VarChar<1>,VarChar<1>,long,long,long,VarChar<25>,VarChar<10>,VarChar<44>,VarChar<1>>,bool>({});
tbb::enumerable_thread_specific<phmap::flat_hash_map<tuple<long,long,long,long,double,double,double,double,VarChar<1>,VarChar<1>,long,long,long,VarChar<25>,VarChar<10>,VarChar<44>,VarChar<1>>,bool>> v282;
auto v269 = phmap::flat_hash_map<long,bool>({});
auto v271 = phmap::flat_hash_map<tuple<long,long,VarChar<1>,double,long,VarChar<15>,VarChar<15>,long,VarChar<79>,VarChar<1>>,bool>({});
tbb::enumerable_thread_specific<phmap::flat_hash_map<tuple<long,long,VarChar<1>,double,long,VarChar<15>,VarChar<15>,long,VarChar<79>,VarChar<1>>,bool>> v289;
auto v273 = phmap::flat_hash_map<tuple<long,long,VarChar<1>,double,long,VarChar<15>,VarChar<15>,long,VarChar<79>,VarChar<1>>,bool>({});
auto v275 = phmap::flat_hash_map<tuple<VarChar<15>>,tuple<double>>({});
auto v277 = phmap::flat_hash_map<tuple<VarChar<15>,double>,bool>({});	
	

auto v278 = db->li_dataset_size; tbb::parallel_for(tbb::blocked_range<size_t>(0, v278), [&](const tbb::blocked_range<size_t>& v279){auto& v283=v282.local();for (size_t v266=v279.begin(), end=v279.end(); v266!=end; ++v266){if ((l_commitdate[v266] < l_receiptdate[v266])){v283[make_tuple(l_orderkey[v266], l_partkey[v266], l_suppkey[v266], l_linenumber[v266], l_quantity[v266], l_extendedprice[v266], l_discount[v266], l_tax[v266], l_returnflag[v266], l_linestatus[v266], l_shipdate[v266], l_commitdate[v266], l_receiptdate[v266], l_shipinstruct[v266], l_shipmode[v266], l_comment[v266], l_NA[v266])] += true;};}});for (auto& local : v282)AddMap<phmap::flat_hash_map<tuple<long,long,long,long,double,double,double,double,VarChar<1>,VarChar<1>,long,long,long,VarChar<25>,VarChar<10>,VarChar<44>,VarChar<1>>,bool>,tuple<long,long,long,long,double,double,double,double,VarChar<1>,VarChar<1>,long,long,long,VarChar<25>,VarChar<10>,VarChar<44>,VarChar<1>>,bool>(v267, local);const auto& lineitem_orders_isin_pre_ops = v267;

for (auto& v268 : lineitem_orders_isin_pre_ops){v269[ /* l_orderkey */get<0>((v268.first))] += true;}const auto& lineitem_orders_isin_build_index = v269;

auto v285 = db->ord_dataset_size; tbb::parallel_for(tbb::blocked_range<size_t>(0, v285), [&](const tbb::blocked_range<size_t>& v286){auto& v290=v289.local();for (size_t v270=v286.begin(), end=v286.end(); v270!=end; ++v270){if (((lineitem_orders_isin_build_index).contains(o_orderkey[v270]))){v290[make_tuple(o_orderkey[v270], o_custkey[v270], o_orderstatus[v270], o_totalprice[v270], o_orderdate[v270], o_orderpriority[v270], o_clerk[v270], o_shippriority[v270], o_comment[v270], o_NA[v270])] += true;};}});for (auto& local : v289)AddMap<phmap::flat_hash_map<tuple<long,long,VarChar<1>,double,long,VarChar<15>,VarChar<15>,long,VarChar<79>,VarChar<1>>,bool>,tuple<long,long,VarChar<1>,double,long,VarChar<15>,VarChar<15>,long,VarChar<79>,VarChar<1>>,bool>(v271, local);const auto& orders_0 = v271;

for (auto& v272 : orders_0){if ((( /* o_orderdate */get<4>((v272.first)) >= (long)19930701) && ( /* o_orderdate */get<4>((v272.first)) < (long)19931001))){v273[(v272.first)] += (v272.second);};}const auto& orders_1 = v273;

for (auto& v274 : orders_1){v275[make_tuple( /* o_orderpriority */get<5>((v274.first)))] += make_tuple((( /* o_orderdate */get<4>((v274.first)) != none)) ? (1.0) : (0.0));}const auto& orders_2 = v275;

for (auto& v276 : orders_2){v277[tuple_cat((v276.first),move((v276.second)))] += true;}const auto& results = v277;const auto& out = results;

    FastDict_s15f_b* result = (FastDict_s15f_b*)PyObject_CallObject(PyObject_CallObject(PyObject_GetAttrString(PyImport_Import(PyUnicode_FromString("test_unopt_fastdict_compiled")), (char*)"new_FastDict_s15f_b"), nullptr), nullptr);
    *(result->dict) = out;

    return (PyObject*)PyObject_CallObject(PyObject_GetAttrString(PyImport_Import(PyUnicode_FromString("sdqlpy.fastd")), "fastd"), Py_BuildValue("(OO)", result, PyUnicode_FromString("test_unopt")));

}

static PyObject * q5(PyObject *self, PyObject* args)
{
	

    PyObject * db_;
    if (!PyArg_ParseTuple(args, "O", &db_)){
        PyErr_SetString(PyExc_ValueError,"Error while parsing the sum inputs.");
        return NULL;
    }
    
    
    DB dbobj;
    DB* db = &dbobj;
    
const static int numpy_initialized =  init_numpy();


	auto li_size = PyArray_Size(PyList_GetItem(PyList_GetItem(db_, 0), 0));
	db->li_dataset_size = li_size;
	auto l_orderkey = (long*)PyArray_DATA(PyList_GetItem(PyList_GetItem(db_, 0), 0));
	auto l_partkey = (long*)PyArray_DATA(PyList_GetItem(PyList_GetItem(db_, 0), 1));
	auto l_suppkey = (long*)PyArray_DATA(PyList_GetItem(PyList_GetItem(db_, 0), 2));
	auto l_linenumber = (long*)PyArray_DATA(PyList_GetItem(PyList_GetItem(db_, 0), 3));
	auto l_quantity = (double*)PyArray_DATA(PyList_GetItem(PyList_GetItem(db_, 0), 4));
	auto l_extendedprice = (double*)PyArray_DATA(PyList_GetItem(PyList_GetItem(db_, 0), 5));
	auto l_discount = (double*)PyArray_DATA(PyList_GetItem(PyList_GetItem(db_, 0), 6));
	auto l_tax = (double*)PyArray_DATA(PyList_GetItem(PyList_GetItem(db_, 0), 7));
	auto l_returnflag= (VarChar<1>*)PyArray_DATA(PyList_GetItem(PyList_GetItem(db_, 0), 8));
	auto l_linestatus= (VarChar<1>*)PyArray_DATA(PyList_GetItem(PyList_GetItem(db_, 0), 9));
	auto l_shipdate = (long*)PyArray_DATA(PyList_GetItem(PyList_GetItem(db_, 0), 10));
	auto l_commitdate = (long*)PyArray_DATA(PyList_GetItem(PyList_GetItem(db_, 0), 11));
	auto l_receiptdate = (long*)PyArray_DATA(PyList_GetItem(PyList_GetItem(db_, 0), 12));
	auto l_shipinstruct= (VarChar<25>*)PyArray_DATA(PyList_GetItem(PyList_GetItem(db_, 0), 13));
	auto l_shipmode= (VarChar<10>*)PyArray_DATA(PyList_GetItem(PyList_GetItem(db_, 0), 14));
	auto l_comment= (VarChar<44>*)PyArray_DATA(PyList_GetItem(PyList_GetItem(db_, 0), 15));
	auto l_NA= (VarChar<1>*)PyArray_DATA(PyList_GetItem(PyList_GetItem(db_, 0), 16));

	auto cu_size = PyArray_Size(PyList_GetItem(PyList_GetItem(db_, 1), 0));
	db->cu_dataset_size = cu_size;
	auto c_custkey = (long*)PyArray_DATA(PyList_GetItem(PyList_GetItem(db_, 1), 0));
	auto c_name= (VarChar<25>*)PyArray_DATA(PyList_GetItem(PyList_GetItem(db_, 1), 1));
	auto c_address= (VarChar<40>*)PyArray_DATA(PyList_GetItem(PyList_GetItem(db_, 1), 2));
	auto c_nationkey = (long*)PyArray_DATA(PyList_GetItem(PyList_GetItem(db_, 1), 3));
	auto c_phone= (VarChar<15>*)PyArray_DATA(PyList_GetItem(PyList_GetItem(db_, 1), 4));
	auto c_acctbal = (double*)PyArray_DATA(PyList_GetItem(PyList_GetItem(db_, 1), 5));
	auto c_mktsegment= (VarChar<10>*)PyArray_DATA(PyList_GetItem(PyList_GetItem(db_, 1), 6));
	auto c_comment= (VarChar<117>*)PyArray_DATA(PyList_GetItem(PyList_GetItem(db_, 1), 7));
	auto c_NA= (VarChar<1>*)PyArray_DATA(PyList_GetItem(PyList_GetItem(db_, 1), 8));

	auto ord_size = PyArray_Size(PyList_GetItem(PyList_GetItem(db_, 2), 0));
	db->ord_dataset_size = ord_size;
	auto o_orderkey = (long*)PyArray_DATA(PyList_GetItem(PyList_GetItem(db_, 2), 0));
	auto o_custkey = (long*)PyArray_DATA(PyList_GetItem(PyList_GetItem(db_, 2), 1));
	auto o_orderstatus= (VarChar<1>*)PyArray_DATA(PyList_GetItem(PyList_GetItem(db_, 2), 2));
	auto o_totalprice = (double*)PyArray_DATA(PyList_GetItem(PyList_GetItem(db_, 2), 3));
	auto o_orderdate = (long*)PyArray_DATA(PyList_GetItem(PyList_GetItem(db_, 2), 4));
	auto o_orderpriority= (VarChar<15>*)PyArray_DATA(PyList_GetItem(PyList_GetItem(db_, 2), 5));
	auto o_clerk= (VarChar<15>*)PyArray_DATA(PyList_GetItem(PyList_GetItem(db_, 2), 6));
	auto o_shippriority = (long*)PyArray_DATA(PyList_GetItem(PyList_GetItem(db_, 2), 7));
	auto o_comment= (VarChar<79>*)PyArray_DATA(PyList_GetItem(PyList_GetItem(db_, 2), 8));
	auto o_NA= (VarChar<1>*)PyArray_DATA(PyList_GetItem(PyList_GetItem(db_, 2), 9));

	auto re_size = PyArray_Size(PyList_GetItem(PyList_GetItem(db_, 3), 0));
	db->re_dataset_size = re_size;
	auto r_regionkey = (long*)PyArray_DATA(PyList_GetItem(PyList_GetItem(db_, 3), 0));
	auto r_name= (VarChar<25>*)PyArray_DATA(PyList_GetItem(PyList_GetItem(db_, 3), 1));
	auto r_comment= (VarChar<152>*)PyArray_DATA(PyList_GetItem(PyList_GetItem(db_, 3), 2));
	auto r_NA= (VarChar<1>*)PyArray_DATA(PyList_GetItem(PyList_GetItem(db_, 3), 3));

	auto na_size = PyArray_Size(PyList_GetItem(PyList_GetItem(db_, 4), 0));
	db->na_dataset_size = na_size;
	auto n_nationkey = (long*)PyArray_DATA(PyList_GetItem(PyList_GetItem(db_, 4), 0));
	auto n_name= (VarChar<25>*)PyArray_DATA(PyList_GetItem(PyList_GetItem(db_, 4), 1));
	auto n_regionkey = (long*)PyArray_DATA(PyList_GetItem(PyList_GetItem(db_, 4), 2));
	auto n_comment= (VarChar<152>*)PyArray_DATA(PyList_GetItem(PyList_GetItem(db_, 4), 3));
	auto n_NA= (VarChar<1>*)PyArray_DATA(PyList_GetItem(PyList_GetItem(db_, 4), 4));

	auto su_size = PyArray_Size(PyList_GetItem(PyList_GetItem(db_, 5), 0));
	db->su_dataset_size = su_size;
	auto s_suppkey = (long*)PyArray_DATA(PyList_GetItem(PyList_GetItem(db_, 5), 0));
	auto s_name= (VarChar<25>*)PyArray_DATA(PyList_GetItem(PyList_GetItem(db_, 5), 1));
	auto s_address= (VarChar<40>*)PyArray_DATA(PyList_GetItem(PyList_GetItem(db_, 5), 2));
	auto s_nationkey = (long*)PyArray_DATA(PyList_GetItem(PyList_GetItem(db_, 5), 3));
	auto s_phone= (VarChar<15>*)PyArray_DATA(PyList_GetItem(PyList_GetItem(db_, 5), 4));
	auto s_acctbal = (double*)PyArray_DATA(PyList_GetItem(PyList_GetItem(db_, 5), 5));
	auto s_comment= (VarChar<101>*)PyArray_DATA(PyList_GetItem(PyList_GetItem(db_, 5), 6));
	auto s_NA= (VarChar<1>*)PyArray_DATA(PyList_GetItem(PyList_GetItem(db_, 5), 7));



	
auto v333 = phmap::flat_hash_map<tuple<long,VarChar<25>,VarChar<152>,VarChar<1>>,bool>({});
tbb::enumerable_thread_specific<phmap::flat_hash_map<tuple<long,VarChar<25>,VarChar<152>,VarChar<1>>,bool>> v376;
auto v335 = phmap::flat_hash_map<long,phmap::flat_hash_map<tuple<long,VarChar<25>,VarChar<152>,VarChar<1>>,bool>>({});
auto v339 = phmap::flat_hash_map<tuple<long,VarChar<25>,long,VarChar<152>,VarChar<1>,long,VarChar<25>,VarChar<152>,VarChar<1>>,bool>({});
tbb::enumerable_thread_specific<phmap::flat_hash_map<tuple<long,VarChar<25>,long,VarChar<152>,VarChar<1>,long,VarChar<25>,VarChar<152>,VarChar<1>>,bool>> v383;
auto v341 = phmap::flat_hash_map<long,phmap::flat_hash_map<tuple<long,VarChar<25>,long,VarChar<152>,VarChar<1>,long,VarChar<25>,VarChar<152>,VarChar<1>>,bool>>({});
auto v345 = phmap::flat_hash_map<tuple<long,VarChar<25>,VarChar<40>,long,VarChar<15>,double,VarChar<10>,VarChar<117>,VarChar<1>,long,VarChar<25>,long,VarChar<152>,VarChar<1>,long,VarChar<25>,VarChar<152>,VarChar<1>>,bool>({});
tbb::enumerable_thread_specific<phmap::flat_hash_map<tuple<long,VarChar<25>,VarChar<40>,long,VarChar<15>,double,VarChar<10>,VarChar<117>,VarChar<1>,long,VarChar<25>,long,VarChar<152>,VarChar<1>,long,VarChar<25>,VarChar<152>,VarChar<1>>,bool>> v390;
auto v347 = phmap::flat_hash_map<tuple<long,long,VarChar<1>,double,long,VarChar<15>,VarChar<15>,long,VarChar<79>,VarChar<1>>,bool>({});
tbb::enumerable_thread_specific<phmap::flat_hash_map<tuple<long,long,VarChar<1>,double,long,VarChar<15>,VarChar<15>,long,VarChar<79>,VarChar<1>>,bool>> v397;
auto v349 = phmap::flat_hash_map<long,phmap::flat_hash_map<tuple<long,VarChar<25>,VarChar<40>,long,VarChar<15>,double,VarChar<10>,VarChar<117>,VarChar<1>,long,VarChar<25>,long,VarChar<152>,VarChar<1>,long,VarChar<25>,VarChar<152>,VarChar<1>>,bool>>({});
auto v353 = phmap::flat_hash_map<tuple<long,long,VarChar<1>,double,long,VarChar<15>,VarChar<15>,long,VarChar<79>,VarChar<1>,long,VarChar<25>,VarChar<40>,long,VarChar<15>,double,VarChar<10>,VarChar<117>,VarChar<1>,long,VarChar<25>,long,VarChar<152>,VarChar<1>,long,VarChar<25>,VarChar<152>,VarChar<1>>,bool>({});
auto v355 = phmap::flat_hash_map<long,phmap::flat_hash_map<tuple<long,long,VarChar<1>,double,long,VarChar<15>,VarChar<15>,long,VarChar<79>,VarChar<1>,long,VarChar<25>,VarChar<40>,long,VarChar<15>,double,VarChar<10>,VarChar<117>,VarChar<1>,long,VarChar<25>,long,VarChar<152>,VarChar<1>,long,VarChar<25>,VarChar<152>,VarChar<1>>,bool>>({});
auto v359 = phmap::flat_hash_map<tuple<long,long,long,long,double,double,double,double,VarChar<1>,VarChar<1>,long,long,long,VarChar<25>,VarChar<10>,VarChar<44>,VarChar<1>,long,long,VarChar<1>,double,long,VarChar<15>,VarChar<15>,long,VarChar<79>,VarChar<1>,long,VarChar<25>,VarChar<40>,long,VarChar<15>,double,VarChar<10>,VarChar<117>,VarChar<1>,long,VarChar<25>,long,VarChar<152>,VarChar<1>,long,VarChar<25>,VarChar<152>,VarChar<1>>,bool>({});
tbb::enumerable_thread_specific<phmap::flat_hash_map<tuple<long,long,long,long,double,double,double,double,VarChar<1>,VarChar<1>,long,long,long,VarChar<25>,VarChar<10>,VarChar<44>,VarChar<1>,long,long,VarChar<1>,double,long,VarChar<15>,VarChar<15>,long,VarChar<79>,VarChar<1>,long,VarChar<25>,VarChar<40>,long,VarChar<15>,double,VarChar<10>,VarChar<117>,VarChar<1>,long,VarChar<25>,long,VarChar<152>,VarChar<1>,long,VarChar<25>,VarChar<152>,VarChar<1>>,bool>> v404;
auto v361 = phmap::flat_hash_map<tuple<long,long>,phmap::flat_hash_map<tuple<long,VarChar<25>,VarChar<40>,long,VarChar<15>,double,VarChar<101>,VarChar<1>>,bool>>({});
tbb::enumerable_thread_specific<phmap::flat_hash_map<tuple<long,long>,phmap::flat_hash_map<tuple<long,VarChar<25>,VarChar<40>,long,VarChar<15>,double,VarChar<101>,VarChar<1>>,bool>>> v411;
auto v365 = phmap::flat_hash_map<tuple<long,long,long,long,double,double,double,double,VarChar<1>,VarChar<1>,long,long,long,VarChar<25>,VarChar<10>,VarChar<44>,VarChar<1>,long,long,VarChar<1>,double,long,VarChar<15>,VarChar<15>,long,VarChar<79>,VarChar<1>,long,VarChar<25>,VarChar<40>,long,VarChar<15>,double,VarChar<10>,VarChar<117>,VarChar<1>,long,VarChar<25>,long,VarChar<152>,VarChar<1>,long,VarChar<25>,VarChar<152>,VarChar<1>,long,VarChar<25>,VarChar<40>,long,VarChar<15>,double,VarChar<101>,VarChar<1>>,bool>({});
auto v367 = phmap::flat_hash_map<tuple<long,long,long,long,double,double,double,double,VarChar<1>,VarChar<1>,long,long,long,VarChar<25>,VarChar<10>,VarChar<44>,VarChar<1>,long,long,VarChar<1>,double,long,VarChar<15>,VarChar<15>,long,VarChar<79>,VarChar<1>,long,VarChar<25>,VarChar<40>,long,VarChar<15>,double,VarChar<10>,VarChar<117>,VarChar<1>,long,VarChar<25>,long,VarChar<152>,VarChar<1>,long,VarChar<25>,VarChar<152>,VarChar<1>,long,VarChar<25>,VarChar<40>,long,VarChar<15>,double,VarChar<101>,VarChar<1>,double>,bool>({});
auto v369 = phmap::flat_hash_map<tuple<VarChar<25>>,tuple<double>>({});
auto v371 = phmap::flat_hash_map<tuple<VarChar<25>,double>,bool>({});	
	const auto& asia = ConstantString("ASIA", 5);

auto v372 = db->re_dataset_size; tbb::parallel_for(tbb::blocked_range<size_t>(0, v372), [&](const tbb::blocked_range<size_t>& v373){auto& v377=v376.local();for (size_t v332=v373.begin(), end=v373.end(); v332!=end; ++v332){if ((r_name[v332] == asia)){v377[make_tuple(r_regionkey[v332], r_name[v332], r_comment[v332], r_NA[v332])] += true;};}});for (auto& local : v376)AddMap<phmap::flat_hash_map<tuple<long,VarChar<25>,VarChar<152>,VarChar<1>>,bool>,tuple<long,VarChar<25>,VarChar<152>,VarChar<1>>,bool>(v333, local);const auto& region_nation_build_pre_ops = v333;

for (auto& v334 : region_nation_build_pre_ops){v335[ /* r_regionkey */get<0>((v334.first))] += phmap::flat_hash_map<tuple<long,VarChar<25>,VarChar<152>,VarChar<1>>,bool>({{(v334.first), (v334.second)}});}const auto& region_nation_build_nest_dict = v335;

auto v379 = db->na_dataset_size; tbb::parallel_for(tbb::blocked_range<size_t>(0, v379), [&](const tbb::blocked_range<size_t>& v380){auto& v384=v383.local();for (size_t v336=v380.begin(), end=v380.end(); v336!=end; ++v336){if (((region_nation_build_nest_dict).contains(n_regionkey[v336]))){for (auto& v337 : (region_nation_build_nest_dict).at(n_regionkey[v336])){v384[tuple_cat(make_tuple(n_nationkey[v336], n_name[v336], n_regionkey[v336], n_comment[v336], n_NA[v336]),move(make_tuple(r_regionkey[v337], r_name[v337], r_comment[v337], r_NA[v337])))] += true;};};}});for (auto& local : v383)AddMap<phmap::flat_hash_map<tuple<long,VarChar<25>,long,VarChar<152>,VarChar<1>,long,VarChar<25>,VarChar<152>,VarChar<1>>,bool>,tuple<long,VarChar<25>,long,VarChar<152>,VarChar<1>,long,VarChar<25>,VarChar<152>,VarChar<1>>,bool>(v339, local);const auto& region_nation_customer_build_pre_ops = v339;

for (auto& v340 : region_nation_customer_build_pre_ops){v341[ /* n_nationkey */get<0>((v340.first))] += phmap::flat_hash_map<tuple<long,VarChar<25>,long,VarChar<152>,VarChar<1>,long,VarChar<25>,VarChar<152>,VarChar<1>>,bool>({{(v340.first), (v340.second)}});}const auto& region_nation_customer_build_nest_dict = v341;

auto v386 = db->cu_dataset_size; tbb::parallel_for(tbb::blocked_range<size_t>(0, v386), [&](const tbb::blocked_range<size_t>& v387){auto& v391=v390.local();for (size_t v342=v387.begin(), end=v387.end(); v342!=end; ++v342){if (((region_nation_customer_build_nest_dict).contains(c_nationkey[v342]))){for (auto& v343 : (region_nation_customer_build_nest_dict).at(c_nationkey[v342])){v391[tuple_cat(make_tuple(c_custkey[v342], c_name[v342], c_address[v342], c_nationkey[v342], c_phone[v342], c_acctbal[v342], c_mktsegment[v342], c_comment[v342], c_NA[v342]),move(make_tuple(n_nationkey[v343], n_name[v343], n_regionkey[v343], n_comment[v343], n_NA[v343], r_regionkey[v343], r_name[v343], r_comment[v343], r_NA[v343])))] += true;};};}});for (auto& local : v390)AddMap<phmap::flat_hash_map<tuple<long,VarChar<25>,VarChar<40>,long,VarChar<15>,double,VarChar<10>,VarChar<117>,VarChar<1>,long,VarChar<25>,long,VarChar<152>,VarChar<1>,long,VarChar<25>,VarChar<152>,VarChar<1>>,bool>,tuple<long,VarChar<25>,VarChar<40>,long,VarChar<15>,double,VarChar<10>,VarChar<117>,VarChar<1>,long,VarChar<25>,long,VarChar<152>,VarChar<1>,long,VarChar<25>,VarChar<152>,VarChar<1>>,bool>(v345, local);const auto& region_nation_customer_orders_build_pre_ops = v345;

auto v393 = db->ord_dataset_size; tbb::parallel_for(tbb::blocked_range<size_t>(0, v393), [&](const tbb::blocked_range<size_t>& v394){auto& v398=v397.local();for (size_t v346=v394.begin(), end=v394.end(); v346!=end; ++v346){if (((o_orderdate[v346] >= (long)19940101) && (o_orderdate[v346] < (long)19950101))){v398[make_tuple(o_orderkey[v346], o_custkey[v346], o_orderstatus[v346], o_totalprice[v346], o_orderdate[v346], o_orderpriority[v346], o_clerk[v346], o_shippriority[v346], o_comment[v346], o_NA[v346])] += true;};}});for (auto& local : v397)AddMap<phmap::flat_hash_map<tuple<long,long,VarChar<1>,double,long,VarChar<15>,VarChar<15>,long,VarChar<79>,VarChar<1>>,bool>,tuple<long,long,VarChar<1>,double,long,VarChar<15>,VarChar<15>,long,VarChar<79>,VarChar<1>>,bool>(v347, local);const auto& region_nation_customer_orders_probe_pre_ops = v347;

for (auto& v348 : region_nation_customer_orders_build_pre_ops){v349[ /* c_custkey */get<0>((v348.first))] += phmap::flat_hash_map<tuple<long,VarChar<25>,VarChar<40>,long,VarChar<15>,double,VarChar<10>,VarChar<117>,VarChar<1>,long,VarChar<25>,long,VarChar<152>,VarChar<1>,long,VarChar<25>,VarChar<152>,VarChar<1>>,bool>({{(v348.first), (v348.second)}});}const auto& region_nation_customer_orders_build_nest_dict = v349;

for (auto& v350 : region_nation_customer_orders_probe_pre_ops){if (((region_nation_customer_orders_build_nest_dict).contains( /* o_custkey */get<1>((v350.first))))){for (auto& v351 : (region_nation_customer_orders_build_nest_dict).at( /* o_custkey */get<1>((v350.first)))){v353[tuple_cat((v350.first),move((v351.first)))] += true;};};}const auto& region_nation_customer_orders_lineitem_build_pre_ops = v353;

for (auto& v354 : region_nation_customer_orders_lineitem_build_pre_ops){v355[ /* o_orderkey */get<0>((v354.first))] += phmap::flat_hash_map<tuple<long,long,VarChar<1>,double,long,VarChar<15>,VarChar<15>,long,VarChar<79>,VarChar<1>,long,VarChar<25>,VarChar<40>,long,VarChar<15>,double,VarChar<10>,VarChar<117>,VarChar<1>,long,VarChar<25>,long,VarChar<152>,VarChar<1>,long,VarChar<25>,VarChar<152>,VarChar<1>>,bool>({{(v354.first), (v354.second)}});}const auto& region_nation_customer_orders_lineitem_build_nest_dict = v355;

auto v400 = db->li_dataset_size; tbb::parallel_for(tbb::blocked_range<size_t>(0, v400), [&](const tbb::blocked_range<size_t>& v401){auto& v405=v404.local();for (size_t v356=v401.begin(), end=v401.end(); v356!=end; ++v356){if (((region_nation_customer_orders_lineitem_build_nest_dict).contains(l_orderkey[v356]))){for (auto& v357 : (region_nation_customer_orders_lineitem_build_nest_dict).at(l_orderkey[v356])){v405[tuple_cat(make_tuple(l_orderkey[v356], l_partkey[v356], l_suppkey[v356], l_linenumber[v356], l_quantity[v356], l_extendedprice[v356], l_discount[v356], l_tax[v356], l_returnflag[v356], l_linestatus[v356], l_shipdate[v356], l_commitdate[v356], l_receiptdate[v356], l_shipinstruct[v356], l_shipmode[v356], l_comment[v356], l_NA[v356]),move(make_tuple(o_orderkey[v357], o_custkey[v357], o_orderstatus[v357], o_totalprice[v357], o_orderdate[v357], o_orderpriority[v357], o_clerk[v357], o_shippriority[v357], o_comment[v357], o_NA[v357], c_custkey[v357], c_name[v357], c_address[v357], c_nationkey[v357], c_phone[v357], c_acctbal[v357], c_mktsegment[v357], c_comment[v357], c_NA[v357], n_nationkey[v357], n_name[v357], n_regionkey[v357], n_comment[v357], n_NA[v357], r_regionkey[v357], r_name[v357], r_comment[v357], r_NA[v357])))] += true;};};}});for (auto& local : v404)AddMap<phmap::flat_hash_map<tuple<long,long,long,long,double,double,double,double,VarChar<1>,VarChar<1>,long,long,long,VarChar<25>,VarChar<10>,VarChar<44>,VarChar<1>,long,long,VarChar<1>,double,long,VarChar<15>,VarChar<15>,long,VarChar<79>,VarChar<1>,long,VarChar<25>,VarChar<40>,long,VarChar<15>,double,VarChar<10>,VarChar<117>,VarChar<1>,long,VarChar<25>,long,VarChar<152>,VarChar<1>,long,VarChar<25>,VarChar<152>,VarChar<1>>,bool>,tuple<long,long,long,long,double,double,double,double,VarChar<1>,VarChar<1>,long,long,long,VarChar<25>,VarChar<10>,VarChar<44>,VarChar<1>,long,long,VarChar<1>,double,long,VarChar<15>,VarChar<15>,long,VarChar<79>,VarChar<1>,long,VarChar<25>,VarChar<40>,long,VarChar<15>,double,VarChar<10>,VarChar<117>,VarChar<1>,long,VarChar<25>,long,VarChar<152>,VarChar<1>,long,VarChar<25>,VarChar<152>,VarChar<1>>,bool>(v359, local);const auto& supplier_region_nation_customer_orders_lineitem_probe_pre_ops = v359;

auto v407 = db->su_dataset_size; tbb::parallel_for(tbb::blocked_range<size_t>(0, v407), [&](const tbb::blocked_range<size_t>& v408){auto& v412=v411.local();for (size_t v360=v408.begin(), end=v408.end(); v360!=end; ++v360){v412[make_tuple(s_suppkey[v360],s_nationkey[v360])] += phmap::flat_hash_map<tuple<long,VarChar<25>,VarChar<40>,long,VarChar<15>,double,VarChar<101>,VarChar<1>>,bool>({{make_tuple(s_suppkey[v360], s_name[v360], s_address[v360], s_nationkey[v360], s_phone[v360], s_acctbal[v360], s_comment[v360], s_NA[v360]), true}});}});for (auto& local : v411)AddMap<phmap::flat_hash_map<tuple<long,long>,phmap::flat_hash_map<tuple<long,VarChar<25>,VarChar<40>,long,VarChar<15>,double,VarChar<101>,VarChar<1>>,bool>>,tuple<long,long>,phmap::flat_hash_map<tuple<long,VarChar<25>,VarChar<40>,long,VarChar<15>,double,VarChar<101>,VarChar<1>>,bool>>(v361, local);const auto& supplier_region_nation_customer_orders_lineitem_build_nest_dict = v361;

for (auto& v362 : supplier_region_nation_customer_orders_lineitem_probe_pre_ops){if (((supplier_region_nation_customer_orders_lineitem_build_nest_dict).contains(make_tuple( /* l_suppkey */get<2>((v362.first)), /* c_nationkey */get<30>((v362.first)))))){for (auto& v363 : (supplier_region_nation_customer_orders_lineitem_build_nest_dict).at(make_tuple( /* l_suppkey */get<2>((v362.first)), /* c_nationkey */get<30>((v362.first))))){v365[tuple_cat((v362.first),move((v363.first)))] += true;};};}const auto& supplier_region_nation_customer_orders_lineitem_0 = v365;

for (auto& v366 : supplier_region_nation_customer_orders_lineitem_0){v367[tuple_cat((v366.first),move(make_tuple(( /* l_extendedprice */get<5>((v366.first)) * (1.0 -  /* l_discount */get<6>((v366.first)))))))] += (v366.second);}const auto& supplier_region_nation_customer_orders_lineitem_1 = v367;

for (auto& v368 : supplier_region_nation_customer_orders_lineitem_1){v369[make_tuple( /* n_name */get<37>((v368.first)))] += make_tuple( /* revenue */get<53>((v368.first)));}const auto& supplier_region_nation_customer_orders_lineitem_2 = v369;

for (auto& v370 : supplier_region_nation_customer_orders_lineitem_2){v371[tuple_cat((v370.first),move((v370.second)))] += true;}const auto& results = v371;const auto& out = results;

    FastDict_s25f_b* result = (FastDict_s25f_b*)PyObject_CallObject(PyObject_CallObject(PyObject_GetAttrString(PyImport_Import(PyUnicode_FromString("test_unopt_fastdict_compiled")), (char*)"new_FastDict_s25f_b"), nullptr), nullptr);
    *(result->dict) = out;

    return (PyObject*)PyObject_CallObject(PyObject_GetAttrString(PyImport_Import(PyUnicode_FromString("sdqlpy.fastd")), "fastd"), Py_BuildValue("(OO)", result, PyUnicode_FromString("test_unopt")));

}

static PyObject * q6(PyObject *self, PyObject* args)
{
	

    PyObject * db_;
    if (!PyArg_ParseTuple(args, "O", &db_)){
        PyErr_SetString(PyExc_ValueError,"Error while parsing the sum inputs.");
        return NULL;
    }
    
    
    DB dbobj;
    DB* db = &dbobj;
    
const static int numpy_initialized =  init_numpy();


	auto li_size = PyArray_Size(PyList_GetItem(PyList_GetItem(db_, 0), 0));
	db->li_dataset_size = li_size;
	auto l_orderkey = (long*)PyArray_DATA(PyList_GetItem(PyList_GetItem(db_, 0), 0));
	auto l_partkey = (long*)PyArray_DATA(PyList_GetItem(PyList_GetItem(db_, 0), 1));
	auto l_suppkey = (long*)PyArray_DATA(PyList_GetItem(PyList_GetItem(db_, 0), 2));
	auto l_linenumber = (long*)PyArray_DATA(PyList_GetItem(PyList_GetItem(db_, 0), 3));
	auto l_quantity = (double*)PyArray_DATA(PyList_GetItem(PyList_GetItem(db_, 0), 4));
	auto l_extendedprice = (double*)PyArray_DATA(PyList_GetItem(PyList_GetItem(db_, 0), 5));
	auto l_discount = (double*)PyArray_DATA(PyList_GetItem(PyList_GetItem(db_, 0), 6));
	auto l_tax = (double*)PyArray_DATA(PyList_GetItem(PyList_GetItem(db_, 0), 7));
	auto l_returnflag= (VarChar<1>*)PyArray_DATA(PyList_GetItem(PyList_GetItem(db_, 0), 8));
	auto l_linestatus= (VarChar<1>*)PyArray_DATA(PyList_GetItem(PyList_GetItem(db_, 0), 9));
	auto l_shipdate = (long*)PyArray_DATA(PyList_GetItem(PyList_GetItem(db_, 0), 10));
	auto l_commitdate = (long*)PyArray_DATA(PyList_GetItem(PyList_GetItem(db_, 0), 11));
	auto l_receiptdate = (long*)PyArray_DATA(PyList_GetItem(PyList_GetItem(db_, 0), 12));
	auto l_shipinstruct= (VarChar<25>*)PyArray_DATA(PyList_GetItem(PyList_GetItem(db_, 0), 13));
	auto l_shipmode= (VarChar<10>*)PyArray_DATA(PyList_GetItem(PyList_GetItem(db_, 0), 14));
	auto l_comment= (VarChar<44>*)PyArray_DATA(PyList_GetItem(PyList_GetItem(db_, 0), 15));
	auto l_NA= (VarChar<1>*)PyArray_DATA(PyList_GetItem(PyList_GetItem(db_, 0), 16));



	
auto v421 = phmap::flat_hash_map<tuple<long,long,long,long,double,double,double,double,VarChar<1>,VarChar<1>,long,long,long,VarChar<25>,VarChar<10>,VarChar<44>,VarChar<1>>,bool>({});
tbb::enumerable_thread_specific<phmap::flat_hash_map<tuple<long,long,long,long,double,double,double,double,VarChar<1>,VarChar<1>,long,long,long,VarChar<25>,VarChar<10>,VarChar<44>,VarChar<1>>,bool>> v430;
auto v423 = phmap::flat_hash_map<tuple<long,long,long,long,double,double,double,double,VarChar<1>,VarChar<1>,long,long,long,VarChar<25>,VarChar<10>,VarChar<44>,VarChar<1>,double>,bool>({});
auto v425 = make_tuple(0.0);	
	

auto v426 = db->li_dataset_size; tbb::parallel_for(tbb::blocked_range<size_t>(0, v426), [&](const tbb::blocked_range<size_t>& v427){auto& v431=v430.local();for (size_t v420=v427.begin(), end=v427.end(); v420!=end; ++v420){if ((((((l_shipdate[v420] >= (long)19940101) && (l_shipdate[v420] < (long)19950101)) && (l_discount[v420] >= 0.05)) && (l_discount[v420] <= 0.07)) && (l_quantity[v420] < (long)24))){v431[make_tuple(l_orderkey[v420], l_partkey[v420], l_suppkey[v420], l_linenumber[v420], l_quantity[v420], l_extendedprice[v420], l_discount[v420], l_tax[v420], l_returnflag[v420], l_linestatus[v420], l_shipdate[v420], l_commitdate[v420], l_receiptdate[v420], l_shipinstruct[v420], l_shipmode[v420], l_comment[v420], l_NA[v420])] += true;};}});for (auto& local : v430)AddMap<phmap::flat_hash_map<tuple<long,long,long,long,double,double,double,double,VarChar<1>,VarChar<1>,long,long,long,VarChar<25>,VarChar<10>,VarChar<44>,VarChar<1>>,bool>,tuple<long,long,long,long,double,double,double,double,VarChar<1>,VarChar<1>,long,long,long,VarChar<25>,VarChar<10>,VarChar<44>,VarChar<1>>,bool>(v421, local);const auto& lineitem_0 = v421;

for (auto& v422 : lineitem_0){v423[tuple_cat((v422.first),move(make_tuple(( /* l_extendedprice */get<5>((v422.first)) *  /* l_discount */get<6>((v422.first))))))] += (v422.second);}const auto& lineitem_1 = v423;

for (auto& v424 : lineitem_1){make_tuple( /* revenue */get<17>((v424.first)));}const auto& lineitem_2 = v425;const auto& results = phmap::flat_hash_map<tuple<double>,bool>({{lineitem_2, true}});const auto& out = results;

    FastDict_f_b* result = (FastDict_f_b*)PyObject_CallObject(PyObject_CallObject(PyObject_GetAttrString(PyImport_Import(PyUnicode_FromString("test_unopt_fastdict_compiled")), (char*)"new_FastDict_f_b"), nullptr), nullptr);
    *(result->dict) = out;

    return (PyObject*)PyObject_CallObject(PyObject_GetAttrString(PyImport_Import(PyUnicode_FromString("sdqlpy.fastd")), "fastd"), Py_BuildValue("(OO)", result, PyUnicode_FromString("test_unopt")));

}

static PyObject * q7(PyObject *self, PyObject* args)
{
	

    PyObject * db_;
    if (!PyArg_ParseTuple(args, "O", &db_)){
        PyErr_SetString(PyExc_ValueError,"Error while parsing the sum inputs.");
        return NULL;
    }
    
    
    DB dbobj;
    DB* db = &dbobj;
    
const static int numpy_initialized =  init_numpy();


	auto su_size = PyArray_Size(PyList_GetItem(PyList_GetItem(db_, 0), 0));
	db->su_dataset_size = su_size;
	auto s_suppkey = (long*)PyArray_DATA(PyList_GetItem(PyList_GetItem(db_, 0), 0));
	auto s_name= (VarChar<25>*)PyArray_DATA(PyList_GetItem(PyList_GetItem(db_, 0), 1));
	auto s_address= (VarChar<40>*)PyArray_DATA(PyList_GetItem(PyList_GetItem(db_, 0), 2));
	auto s_nationkey = (long*)PyArray_DATA(PyList_GetItem(PyList_GetItem(db_, 0), 3));
	auto s_phone= (VarChar<15>*)PyArray_DATA(PyList_GetItem(PyList_GetItem(db_, 0), 4));
	auto s_acctbal = (double*)PyArray_DATA(PyList_GetItem(PyList_GetItem(db_, 0), 5));
	auto s_comment= (VarChar<101>*)PyArray_DATA(PyList_GetItem(PyList_GetItem(db_, 0), 6));
	auto s_NA= (VarChar<1>*)PyArray_DATA(PyList_GetItem(PyList_GetItem(db_, 0), 7));

	auto li_size = PyArray_Size(PyList_GetItem(PyList_GetItem(db_, 1), 0));
	db->li_dataset_size = li_size;
	auto l_orderkey = (long*)PyArray_DATA(PyList_GetItem(PyList_GetItem(db_, 1), 0));
	auto l_partkey = (long*)PyArray_DATA(PyList_GetItem(PyList_GetItem(db_, 1), 1));
	auto l_suppkey = (long*)PyArray_DATA(PyList_GetItem(PyList_GetItem(db_, 1), 2));
	auto l_linenumber = (long*)PyArray_DATA(PyList_GetItem(PyList_GetItem(db_, 1), 3));
	auto l_quantity = (double*)PyArray_DATA(PyList_GetItem(PyList_GetItem(db_, 1), 4));
	auto l_extendedprice = (double*)PyArray_DATA(PyList_GetItem(PyList_GetItem(db_, 1), 5));
	auto l_discount = (double*)PyArray_DATA(PyList_GetItem(PyList_GetItem(db_, 1), 6));
	auto l_tax = (double*)PyArray_DATA(PyList_GetItem(PyList_GetItem(db_, 1), 7));
	auto l_returnflag= (VarChar<1>*)PyArray_DATA(PyList_GetItem(PyList_GetItem(db_, 1), 8));
	auto l_linestatus= (VarChar<1>*)PyArray_DATA(PyList_GetItem(PyList_GetItem(db_, 1), 9));
	auto l_shipdate = (long*)PyArray_DATA(PyList_GetItem(PyList_GetItem(db_, 1), 10));
	auto l_commitdate = (long*)PyArray_DATA(PyList_GetItem(PyList_GetItem(db_, 1), 11));
	auto l_receiptdate = (long*)PyArray_DATA(PyList_GetItem(PyList_GetItem(db_, 1), 12));
	auto l_shipinstruct= (VarChar<25>*)PyArray_DATA(PyList_GetItem(PyList_GetItem(db_, 1), 13));
	auto l_shipmode= (VarChar<10>*)PyArray_DATA(PyList_GetItem(PyList_GetItem(db_, 1), 14));
	auto l_comment= (VarChar<44>*)PyArray_DATA(PyList_GetItem(PyList_GetItem(db_, 1), 15));
	auto l_NA= (VarChar<1>*)PyArray_DATA(PyList_GetItem(PyList_GetItem(db_, 1), 16));

	auto ord_size = PyArray_Size(PyList_GetItem(PyList_GetItem(db_, 2), 0));
	db->ord_dataset_size = ord_size;
	auto o_orderkey = (long*)PyArray_DATA(PyList_GetItem(PyList_GetItem(db_, 2), 0));
	auto o_custkey = (long*)PyArray_DATA(PyList_GetItem(PyList_GetItem(db_, 2), 1));
	auto o_orderstatus= (VarChar<1>*)PyArray_DATA(PyList_GetItem(PyList_GetItem(db_, 2), 2));
	auto o_totalprice = (double*)PyArray_DATA(PyList_GetItem(PyList_GetItem(db_, 2), 3));
	auto o_orderdate = (long*)PyArray_DATA(PyList_GetItem(PyList_GetItem(db_, 2), 4));
	auto o_orderpriority= (VarChar<15>*)PyArray_DATA(PyList_GetItem(PyList_GetItem(db_, 2), 5));
	auto o_clerk= (VarChar<15>*)PyArray_DATA(PyList_GetItem(PyList_GetItem(db_, 2), 6));
	auto o_shippriority = (long*)PyArray_DATA(PyList_GetItem(PyList_GetItem(db_, 2), 7));
	auto o_comment= (VarChar<79>*)PyArray_DATA(PyList_GetItem(PyList_GetItem(db_, 2), 8));
	auto o_NA= (VarChar<1>*)PyArray_DATA(PyList_GetItem(PyList_GetItem(db_, 2), 9));

	auto cu_size = PyArray_Size(PyList_GetItem(PyList_GetItem(db_, 3), 0));
	db->cu_dataset_size = cu_size;
	auto c_custkey = (long*)PyArray_DATA(PyList_GetItem(PyList_GetItem(db_, 3), 0));
	auto c_name= (VarChar<25>*)PyArray_DATA(PyList_GetItem(PyList_GetItem(db_, 3), 1));
	auto c_address= (VarChar<40>*)PyArray_DATA(PyList_GetItem(PyList_GetItem(db_, 3), 2));
	auto c_nationkey = (long*)PyArray_DATA(PyList_GetItem(PyList_GetItem(db_, 3), 3));
	auto c_phone= (VarChar<15>*)PyArray_DATA(PyList_GetItem(PyList_GetItem(db_, 3), 4));
	auto c_acctbal = (double*)PyArray_DATA(PyList_GetItem(PyList_GetItem(db_, 3), 5));
	auto c_mktsegment= (VarChar<10>*)PyArray_DATA(PyList_GetItem(PyList_GetItem(db_, 3), 6));
	auto c_comment= (VarChar<117>*)PyArray_DATA(PyList_GetItem(PyList_GetItem(db_, 3), 7));
	auto c_NA= (VarChar<1>*)PyArray_DATA(PyList_GetItem(PyList_GetItem(db_, 3), 8));

	auto na_size = PyArray_Size(PyList_GetItem(PyList_GetItem(db_, 4), 0));
	db->na_dataset_size = na_size;
	auto n_nationkey = (long*)PyArray_DATA(PyList_GetItem(PyList_GetItem(db_, 4), 0));
	auto n_name= (VarChar<25>*)PyArray_DATA(PyList_GetItem(PyList_GetItem(db_, 4), 1));
	auto n_regionkey = (long*)PyArray_DATA(PyList_GetItem(PyList_GetItem(db_, 4), 2));
	auto n_comment= (VarChar<152>*)PyArray_DATA(PyList_GetItem(PyList_GetItem(db_, 4), 3));
	auto n_NA= (VarChar<1>*)PyArray_DATA(PyList_GetItem(PyList_GetItem(db_, 4), 4));



	
auto v488 = phmap::flat_hash_map<tuple<long,VarChar<25>,long,VarChar<152>,VarChar<1>>,bool>({});
tbb::enumerable_thread_specific<phmap::flat_hash_map<tuple<long,VarChar<25>,long,VarChar<152>,VarChar<1>>,bool>> v545;
auto v490 = phmap::flat_hash_map<long,phmap::flat_hash_map<tuple<long,VarChar<25>,long,VarChar<152>,VarChar<1>>,bool>>({});
auto v494 = phmap::flat_hash_map<tuple<long,VarChar<25>,VarChar<40>,long,VarChar<15>,double,VarChar<101>,VarChar<1>,long,VarChar<25>,long,VarChar<152>,VarChar<1>>,bool>({});
tbb::enumerable_thread_specific<phmap::flat_hash_map<tuple<long,VarChar<25>,VarChar<40>,long,VarChar<15>,double,VarChar<101>,VarChar<1>,long,VarChar<25>,long,VarChar<152>,VarChar<1>>,bool>> v552;
auto v496 = phmap::flat_hash_map<tuple<long,VarChar<25>,VarChar<40>,long,VarChar<15>,double,VarChar<101>,VarChar<1>,long,VarChar<25>,long,VarChar<152>,VarChar<1>,VarChar<25>>,bool>({});
auto v498 = phmap::flat_hash_map<tuple<long,VarChar<25>,long,VarChar<152>,VarChar<1>>,bool>({});
tbb::enumerable_thread_specific<phmap::flat_hash_map<tuple<long,VarChar<25>,long,VarChar<152>,VarChar<1>>,bool>> v559;
auto v500 = phmap::flat_hash_map<long,phmap::flat_hash_map<tuple<long,VarChar<25>,long,VarChar<152>,VarChar<1>>,bool>>({});
auto v504 = phmap::flat_hash_map<tuple<long,VarChar<25>,VarChar<40>,long,VarChar<15>,double,VarChar<10>,VarChar<117>,VarChar<1>,long,VarChar<25>,long,VarChar<152>,VarChar<1>>,bool>({});
tbb::enumerable_thread_specific<phmap::flat_hash_map<tuple<long,VarChar<25>,VarChar<40>,long,VarChar<15>,double,VarChar<10>,VarChar<117>,VarChar<1>,long,VarChar<25>,long,VarChar<152>,VarChar<1>>,bool>> v566;
auto v506 = phmap::flat_hash_map<long,phmap::flat_hash_map<tuple<long,VarChar<25>,VarChar<40>,long,VarChar<15>,double,VarChar<10>,VarChar<117>,VarChar<1>,long,VarChar<25>,long,VarChar<152>,VarChar<1>>,bool>>({});
auto v510 = phmap::flat_hash_map<tuple<long,long,VarChar<1>,double,long,VarChar<15>,VarChar<15>,long,VarChar<79>,VarChar<1>,long,VarChar<25>,VarChar<40>,long,VarChar<15>,double,VarChar<10>,VarChar<117>,VarChar<1>,long,VarChar<25>,long,VarChar<152>,VarChar<1>>,bool>({});
tbb::enumerable_thread_specific<phmap::flat_hash_map<tuple<long,long,VarChar<1>,double,long,VarChar<15>,VarChar<15>,long,VarChar<79>,VarChar<1>,long,VarChar<25>,VarChar<40>,long,VarChar<15>,double,VarChar<10>,VarChar<117>,VarChar<1>,long,VarChar<25>,long,VarChar<152>,VarChar<1>>,bool>> v573;
auto v512 = phmap::flat_hash_map<tuple<long,long,VarChar<1>,double,long,VarChar<15>,VarChar<15>,long,VarChar<79>,VarChar<1>,long,VarChar<25>,VarChar<40>,long,VarChar<15>,double,VarChar<10>,VarChar<117>,VarChar<1>,long,VarChar<25>,long,VarChar<152>,VarChar<1>,VarChar<25>>,bool>({});
auto v514 = phmap::flat_hash_map<tuple<long,long,long,long,double,double,double,double,VarChar<1>,VarChar<1>,long,long,long,VarChar<25>,VarChar<10>,VarChar<44>,VarChar<1>>,bool>({});
tbb::enumerable_thread_specific<phmap::flat_hash_map<tuple<long,long,long,long,double,double,double,double,VarChar<1>,VarChar<1>,long,long,long,VarChar<25>,VarChar<10>,VarChar<44>,VarChar<1>>,bool>> v580;
auto v516 = phmap::flat_hash_map<long,phmap::flat_hash_map<tuple<long,long,VarChar<1>,double,long,VarChar<15>,VarChar<15>,long,VarChar<79>,VarChar<1>,long,VarChar<25>,VarChar<40>,long,VarChar<15>,double,VarChar<10>,VarChar<117>,VarChar<1>,long,VarChar<25>,long,VarChar<152>,VarChar<1>,VarChar<25>>,bool>>({});
auto v520 = phmap::flat_hash_map<tuple<long,long,long,long,double,double,double,double,VarChar<1>,VarChar<1>,long,long,long,VarChar<25>,VarChar<10>,VarChar<44>,VarChar<1>,long,long,VarChar<1>,double,long,VarChar<15>,VarChar<15>,long,VarChar<79>,VarChar<1>,long,VarChar<25>,VarChar<40>,long,VarChar<15>,double,VarChar<10>,VarChar<117>,VarChar<1>,long,VarChar<25>,long,VarChar<152>,VarChar<1>,VarChar<25>>,bool>({});
auto v522 = phmap::flat_hash_map<long,phmap::flat_hash_map<tuple<long,VarChar<25>,VarChar<40>,long,VarChar<15>,double,VarChar<101>,VarChar<1>,long,VarChar<25>,long,VarChar<152>,VarChar<1>,VarChar<25>>,bool>>({});
auto v526 = phmap::flat_hash_map<tuple<long,long,long,long,double,double,double,double,VarChar<1>,VarChar<1>,long,long,long,VarChar<25>,VarChar<10>,VarChar<44>,VarChar<1>,long,long,VarChar<1>,double,long,VarChar<15>,VarChar<15>,long,VarChar<79>,VarChar<1>,long,VarChar<25>,VarChar<40>,long,VarChar<15>,double,VarChar<10>,VarChar<117>,VarChar<1>,long,VarChar<25>,long,VarChar<152>,VarChar<1>,VarChar<25>,long,VarChar<25>,VarChar<40>,long,VarChar<15>,double,VarChar<101>,VarChar<1>,long,VarChar<25>,long,VarChar<152>,VarChar<1>,VarChar<25>>,bool>({});
auto v528 = phmap::flat_hash_map<tuple<long,long,long,long,double,double,double,double,VarChar<1>,VarChar<1>,long,long,long,VarChar<25>,VarChar<10>,VarChar<44>,VarChar<1>,long,long,VarChar<1>,double,long,VarChar<15>,VarChar<15>,long,VarChar<79>,VarChar<1>,long,VarChar<25>,VarChar<40>,long,VarChar<15>,double,VarChar<10>,VarChar<117>,VarChar<1>,long,VarChar<25>,long,VarChar<152>,VarChar<1>,VarChar<25>,long,VarChar<25>,VarChar<40>,long,VarChar<15>,double,VarChar<101>,VarChar<1>,long,VarChar<25>,long,VarChar<152>,VarChar<1>,VarChar<25>>,bool>({});
auto v530 = phmap::flat_hash_map<tuple<long,long,long,long,double,double,double,double,VarChar<1>,VarChar<1>,long,long,long,VarChar<25>,VarChar<10>,VarChar<44>,VarChar<1>,long,long,VarChar<1>,double,long,VarChar<15>,VarChar<15>,long,VarChar<79>,VarChar<1>,long,VarChar<25>,VarChar<40>,long,VarChar<15>,double,VarChar<10>,VarChar<117>,VarChar<1>,long,VarChar<25>,long,VarChar<152>,VarChar<1>,VarChar<25>,long,VarChar<25>,VarChar<40>,long,VarChar<15>,double,VarChar<101>,VarChar<1>,long,VarChar<25>,long,VarChar<152>,VarChar<1>,VarChar<25>,VarChar<25>>,bool>({});
auto v532 = phmap::flat_hash_map<tuple<long,long,long,long,double,double,double,double,VarChar<1>,VarChar<1>,long,long,long,VarChar<25>,VarChar<10>,VarChar<44>,VarChar<1>,long,long,VarChar<1>,double,long,VarChar<15>,VarChar<15>,long,VarChar<79>,VarChar<1>,long,VarChar<25>,VarChar<40>,long,VarChar<15>,double,VarChar<10>,VarChar<117>,VarChar<1>,long,VarChar<25>,long,VarChar<152>,VarChar<1>,VarChar<25>,long,VarChar<25>,VarChar<40>,long,VarChar<15>,double,VarChar<101>,VarChar<1>,long,VarChar<25>,long,VarChar<152>,VarChar<1>,VarChar<25>,VarChar<25>,VarChar<25>>,bool>({});
auto v534 = phmap::flat_hash_map<tuple<long,long,long,long,double,double,double,double,VarChar<1>,VarChar<1>,long,long,long,VarChar<25>,VarChar<10>,VarChar<44>,VarChar<1>,long,long,VarChar<1>,double,long,VarChar<15>,VarChar<15>,long,VarChar<79>,VarChar<1>,long,VarChar<25>,VarChar<40>,long,VarChar<15>,double,VarChar<10>,VarChar<117>,VarChar<1>,long,VarChar<25>,long,VarChar<152>,VarChar<1>,VarChar<25>,long,VarChar<25>,VarChar<40>,long,VarChar<15>,double,VarChar<101>,VarChar<1>,long,VarChar<25>,long,VarChar<152>,VarChar<1>,VarChar<25>,VarChar<25>,VarChar<25>,long>,bool>({});
auto v536 = phmap::flat_hash_map<tuple<long,long,long,long,double,double,double,double,VarChar<1>,VarChar<1>,long,long,long,VarChar<25>,VarChar<10>,VarChar<44>,VarChar<1>,long,long,VarChar<1>,double,long,VarChar<15>,VarChar<15>,long,VarChar<79>,VarChar<1>,long,VarChar<25>,VarChar<40>,long,VarChar<15>,double,VarChar<10>,VarChar<117>,VarChar<1>,long,VarChar<25>,long,VarChar<152>,VarChar<1>,VarChar<25>,long,VarChar<25>,VarChar<40>,long,VarChar<15>,double,VarChar<101>,VarChar<1>,long,VarChar<25>,long,VarChar<152>,VarChar<1>,VarChar<25>,VarChar<25>,VarChar<25>,long,double>,bool>({});
auto v538 = phmap::flat_hash_map<tuple<VarChar<25>,VarChar<25>,long>,tuple<double>>({});
auto v540 = phmap::flat_hash_map<tuple<VarChar<25>,VarChar<25>,long,double>,bool>({});	
	const auto& france = ConstantString("FRANCE", 7);const auto& germany = ConstantString("GERMANY", 8);

auto v541 = db->na_dataset_size; tbb::parallel_for(tbb::blocked_range<size_t>(0, v541), [&](const tbb::blocked_range<size_t>& v542){auto& v546=v545.local();for (size_t v487=v542.begin(), end=v542.end(); v487!=end; ++v487){if (((n_name[v487] == france) || (n_name[v487] == germany))){v546[make_tuple(n_nationkey[v487], n_name[v487], n_regionkey[v487], n_comment[v487], n_NA[v487])] += true;};}});for (auto& local : v545)AddMap<phmap::flat_hash_map<tuple<long,VarChar<25>,long,VarChar<152>,VarChar<1>>,bool>,tuple<long,VarChar<25>,long,VarChar<152>,VarChar<1>>,bool>(v488, local);const auto& n1_supplier_build_pre_ops = v488;

for (auto& v489 : n1_supplier_build_pre_ops){v490[ /* n_nationkey */get<0>((v489.first))] += phmap::flat_hash_map<tuple<long,VarChar<25>,long,VarChar<152>,VarChar<1>>,bool>({{(v489.first), (v489.second)}});}const auto& n1_supplier_build_nest_dict = v490;

auto v548 = db->su_dataset_size; tbb::parallel_for(tbb::blocked_range<size_t>(0, v548), [&](const tbb::blocked_range<size_t>& v549){auto& v553=v552.local();for (size_t v491=v549.begin(), end=v549.end(); v491!=end; ++v491){if (((n1_supplier_build_nest_dict).contains(s_nationkey[v491]))){for (auto& v492 : (n1_supplier_build_nest_dict).at(s_nationkey[v491])){v553[tuple_cat(make_tuple(s_suppkey[v491], s_name[v491], s_address[v491], s_nationkey[v491], s_phone[v491], s_acctbal[v491], s_comment[v491], s_NA[v491]),move(make_tuple(n_nationkey[v492], n_name[v492], n_regionkey[v492], n_comment[v492], n_NA[v492])))] += true;};};}});for (auto& local : v552)AddMap<phmap::flat_hash_map<tuple<long,VarChar<25>,VarChar<40>,long,VarChar<15>,double,VarChar<101>,VarChar<1>,long,VarChar<25>,long,VarChar<152>,VarChar<1>>,bool>,tuple<long,VarChar<25>,VarChar<40>,long,VarChar<15>,double,VarChar<101>,VarChar<1>,long,VarChar<25>,long,VarChar<152>,VarChar<1>>,bool>(v494, local);const auto& n1_supplier_0 = v494;

for (auto& v495 : n1_supplier_0){v496[tuple_cat((v495.first),move(make_tuple( /* n_name */get<9>((v495.first)))))] += (v495.second);}const auto& n1_supplier_n2_customer_orders_lineitem_build_pre_ops = v496;

auto v555 = db->na_dataset_size; tbb::parallel_for(tbb::blocked_range<size_t>(0, v555), [&](const tbb::blocked_range<size_t>& v556){auto& v560=v559.local();for (size_t v497=v556.begin(), end=v556.end(); v497!=end; ++v497){if (((n_name[v497] == france) || (n_name[v497] == germany))){v560[make_tuple(n_nationkey[v497], n_name[v497], n_regionkey[v497], n_comment[v497], n_NA[v497])] += true;};}});for (auto& local : v559)AddMap<phmap::flat_hash_map<tuple<long,VarChar<25>,long,VarChar<152>,VarChar<1>>,bool>,tuple<long,VarChar<25>,long,VarChar<152>,VarChar<1>>,bool>(v498, local);const auto& n2_customer_build_pre_ops = v498;

for (auto& v499 : n2_customer_build_pre_ops){v500[ /* n_nationkey */get<0>((v499.first))] += phmap::flat_hash_map<tuple<long,VarChar<25>,long,VarChar<152>,VarChar<1>>,bool>({{(v499.first), (v499.second)}});}const auto& n2_customer_build_nest_dict = v500;

auto v562 = db->cu_dataset_size; tbb::parallel_for(tbb::blocked_range<size_t>(0, v562), [&](const tbb::blocked_range<size_t>& v563){auto& v567=v566.local();for (size_t v501=v563.begin(), end=v563.end(); v501!=end; ++v501){if (((n2_customer_build_nest_dict).contains(c_nationkey[v501]))){for (auto& v502 : (n2_customer_build_nest_dict).at(c_nationkey[v501])){v567[tuple_cat(make_tuple(c_custkey[v501], c_name[v501], c_address[v501], c_nationkey[v501], c_phone[v501], c_acctbal[v501], c_mktsegment[v501], c_comment[v501], c_NA[v501]),move(make_tuple(n_nationkey[v502], n_name[v502], n_regionkey[v502], n_comment[v502], n_NA[v502])))] += true;};};}});for (auto& local : v566)AddMap<phmap::flat_hash_map<tuple<long,VarChar<25>,VarChar<40>,long,VarChar<15>,double,VarChar<10>,VarChar<117>,VarChar<1>,long,VarChar<25>,long,VarChar<152>,VarChar<1>>,bool>,tuple<long,VarChar<25>,VarChar<40>,long,VarChar<15>,double,VarChar<10>,VarChar<117>,VarChar<1>,long,VarChar<25>,long,VarChar<152>,VarChar<1>>,bool>(v504, local);const auto& n2_customer_orders_build_pre_ops = v504;

for (auto& v505 : n2_customer_orders_build_pre_ops){v506[ /* c_custkey */get<0>((v505.first))] += phmap::flat_hash_map<tuple<long,VarChar<25>,VarChar<40>,long,VarChar<15>,double,VarChar<10>,VarChar<117>,VarChar<1>,long,VarChar<25>,long,VarChar<152>,VarChar<1>>,bool>({{(v505.first), (v505.second)}});}const auto& n2_customer_orders_build_nest_dict = v506;

auto v569 = db->ord_dataset_size; tbb::parallel_for(tbb::blocked_range<size_t>(0, v569), [&](const tbb::blocked_range<size_t>& v570){auto& v574=v573.local();for (size_t v507=v570.begin(), end=v570.end(); v507!=end; ++v507){if (((n2_customer_orders_build_nest_dict).contains(o_custkey[v507]))){for (auto& v508 : (n2_customer_orders_build_nest_dict).at(o_custkey[v507])){v574[tuple_cat(make_tuple(o_orderkey[v507], o_custkey[v507], o_orderstatus[v507], o_totalprice[v507], o_orderdate[v507], o_orderpriority[v507], o_clerk[v507], o_shippriority[v507], o_comment[v507], o_NA[v507]),move(make_tuple(c_custkey[v508], c_name[v508], c_address[v508], c_nationkey[v508], c_phone[v508], c_acctbal[v508], c_mktsegment[v508], c_comment[v508], c_NA[v508], n_nationkey[v508], n_name[v508], n_regionkey[v508], n_comment[v508], n_NA[v508])))] += true;};};}});for (auto& local : v573)AddMap<phmap::flat_hash_map<tuple<long,long,VarChar<1>,double,long,VarChar<15>,VarChar<15>,long,VarChar<79>,VarChar<1>,long,VarChar<25>,VarChar<40>,long,VarChar<15>,double,VarChar<10>,VarChar<117>,VarChar<1>,long,VarChar<25>,long,VarChar<152>,VarChar<1>>,bool>,tuple<long,long,VarChar<1>,double,long,VarChar<15>,VarChar<15>,long,VarChar<79>,VarChar<1>,long,VarChar<25>,VarChar<40>,long,VarChar<15>,double,VarChar<10>,VarChar<117>,VarChar<1>,long,VarChar<25>,long,VarChar<152>,VarChar<1>>,bool>(v510, local);const auto& n2_customer_orders_0 = v510;

for (auto& v511 : n2_customer_orders_0){v512[tuple_cat((v511.first),move(make_tuple( /* n_name */get<20>((v511.first)))))] += (v511.second);}const auto& n2_customer_orders_lineitem_build_pre_ops = v512;

auto v576 = db->li_dataset_size; tbb::parallel_for(tbb::blocked_range<size_t>(0, v576), [&](const tbb::blocked_range<size_t>& v577){auto& v581=v580.local();for (size_t v513=v577.begin(), end=v577.end(); v513!=end; ++v513){if (((l_shipdate[v513] >= (long)19950101) && (l_shipdate[v513] <= (long)19961231))){v581[make_tuple(l_orderkey[v513], l_partkey[v513], l_suppkey[v513], l_linenumber[v513], l_quantity[v513], l_extendedprice[v513], l_discount[v513], l_tax[v513], l_returnflag[v513], l_linestatus[v513], l_shipdate[v513], l_commitdate[v513], l_receiptdate[v513], l_shipinstruct[v513], l_shipmode[v513], l_comment[v513], l_NA[v513])] += true;};}});for (auto& local : v580)AddMap<phmap::flat_hash_map<tuple<long,long,long,long,double,double,double,double,VarChar<1>,VarChar<1>,long,long,long,VarChar<25>,VarChar<10>,VarChar<44>,VarChar<1>>,bool>,tuple<long,long,long,long,double,double,double,double,VarChar<1>,VarChar<1>,long,long,long,VarChar<25>,VarChar<10>,VarChar<44>,VarChar<1>>,bool>(v514, local);const auto& n2_customer_orders_lineitem_probe_pre_ops = v514;

for (auto& v515 : n2_customer_orders_lineitem_build_pre_ops){v516[ /* o_orderkey */get<0>((v515.first))] += phmap::flat_hash_map<tuple<long,long,VarChar<1>,double,long,VarChar<15>,VarChar<15>,long,VarChar<79>,VarChar<1>,long,VarChar<25>,VarChar<40>,long,VarChar<15>,double,VarChar<10>,VarChar<117>,VarChar<1>,long,VarChar<25>,long,VarChar<152>,VarChar<1>,VarChar<25>>,bool>({{(v515.first), (v515.second)}});}const auto& n2_customer_orders_lineitem_build_nest_dict = v516;

for (auto& v517 : n2_customer_orders_lineitem_probe_pre_ops){if (((n2_customer_orders_lineitem_build_nest_dict).contains( /* l_orderkey */get<0>((v517.first))))){for (auto& v518 : (n2_customer_orders_lineitem_build_nest_dict).at( /* l_orderkey */get<0>((v517.first)))){v520[tuple_cat((v517.first),move((v518.first)))] += true;};};}const auto& n1_supplier_n2_customer_orders_lineitem_probe_pre_ops = v520;

for (auto& v521 : n1_supplier_n2_customer_orders_lineitem_build_pre_ops){v522[ /* s_suppkey */get<0>((v521.first))] += phmap::flat_hash_map<tuple<long,VarChar<25>,VarChar<40>,long,VarChar<15>,double,VarChar<101>,VarChar<1>,long,VarChar<25>,long,VarChar<152>,VarChar<1>,VarChar<25>>,bool>({{(v521.first), (v521.second)}});}const auto& n1_supplier_n2_customer_orders_lineitem_build_nest_dict = v522;

for (auto& v523 : n1_supplier_n2_customer_orders_lineitem_probe_pre_ops){if (((n1_supplier_n2_customer_orders_lineitem_build_nest_dict).contains( /* l_suppkey */get<2>((v523.first))))){for (auto& v524 : (n1_supplier_n2_customer_orders_lineitem_build_nest_dict).at( /* l_suppkey */get<2>((v523.first)))){v526[tuple_cat((v523.first),move((v524.first)))] += true;};};}const auto& n1_supplier_n2_customer_orders_lineitem_0 = v526;

for (auto& v527 : n1_supplier_n2_customer_orders_lineitem_0){if (((( /* n1_name */get<55>((v527.first)) == france) && ( /* n2_name */get<41>((v527.first)) == germany)) || (( /* n1_name */get<55>((v527.first)) == germany) && ( /* n2_name */get<41>((v527.first)) == france)))){v528[(v527.first)] += (v527.second);};}const auto& n1_supplier_n2_customer_orders_lineitem_1 = v528;

for (auto& v529 : n1_supplier_n2_customer_orders_lineitem_1){v530[tuple_cat((v529.first),move(make_tuple( /* n1_name */get<55>((v529.first)))))] += (v529.second);}const auto& n1_supplier_n2_customer_orders_lineitem_2 = v530;

for (auto& v531 : n1_supplier_n2_customer_orders_lineitem_2){v532[tuple_cat((v531.first),move(make_tuple( /* n2_name */get<41>((v531.first)))))] += (v531.second);}const auto& n1_supplier_n2_customer_orders_lineitem_3 = v532;

for (auto& v533 : n1_supplier_n2_customer_orders_lineitem_3){v534[tuple_cat((v533.first),move(make_tuple((( /* l_shipdate */get<10>((v533.first)))/10000))))] += (v533.second);}const auto& n1_supplier_n2_customer_orders_lineitem_4 = v534;

for (auto& v535 : n1_supplier_n2_customer_orders_lineitem_4){v536[tuple_cat((v535.first),move(make_tuple(( /* l_extendedprice */get<5>((v535.first)) * (1.0 -  /* l_discount */get<6>((v535.first)))))))] += (v535.second);}const auto& n1_supplier_n2_customer_orders_lineitem_5 = v536;

for (auto& v537 : n1_supplier_n2_customer_orders_lineitem_5){v538[make_tuple( /* supp_nation */get<56>((v537.first)), /* cust_nation */get<57>((v537.first)), /* l_year */get<58>((v537.first)))] += make_tuple( /* volume */get<59>((v537.first)));}const auto& n1_supplier_n2_customer_orders_lineitem_6 = v538;

for (auto& v539 : n1_supplier_n2_customer_orders_lineitem_6){v540[tuple_cat((v539.first),move((v539.second)))] += true;}const auto& results = v540;const auto& out = results;

    FastDict_s25s25if_b* result = (FastDict_s25s25if_b*)PyObject_CallObject(PyObject_CallObject(PyObject_GetAttrString(PyImport_Import(PyUnicode_FromString("test_unopt_fastdict_compiled")), (char*)"new_FastDict_s25s25if_b"), nullptr), nullptr);
    *(result->dict) = out;

    return (PyObject*)PyObject_CallObject(PyObject_GetAttrString(PyImport_Import(PyUnicode_FromString("sdqlpy.fastd")), "fastd"), Py_BuildValue("(OO)", result, PyUnicode_FromString("test_unopt")));

}

static PyObject * q8(PyObject *self, PyObject* args)
{
	

    PyObject * db_;
    if (!PyArg_ParseTuple(args, "O", &db_)){
        PyErr_SetString(PyExc_ValueError,"Error while parsing the sum inputs.");
        return NULL;
    }
    
    
    DB dbobj;
    DB* db = &dbobj;
    
const static int numpy_initialized =  init_numpy();


	auto pa_size = PyArray_Size(PyList_GetItem(PyList_GetItem(db_, 0), 0));
	db->pa_dataset_size = pa_size;
	auto p_partkey = (long*)PyArray_DATA(PyList_GetItem(PyList_GetItem(db_, 0), 0));
	auto p_name= (VarChar<55>*)PyArray_DATA(PyList_GetItem(PyList_GetItem(db_, 0), 1));
	auto p_mfgr= (VarChar<25>*)PyArray_DATA(PyList_GetItem(PyList_GetItem(db_, 0), 2));
	auto p_brand= (VarChar<10>*)PyArray_DATA(PyList_GetItem(PyList_GetItem(db_, 0), 3));
	auto p_type= (VarChar<25>*)PyArray_DATA(PyList_GetItem(PyList_GetItem(db_, 0), 4));
	auto p_size = (long*)PyArray_DATA(PyList_GetItem(PyList_GetItem(db_, 0), 5));
	auto p_container= (VarChar<10>*)PyArray_DATA(PyList_GetItem(PyList_GetItem(db_, 0), 6));
	auto p_retailprice = (double*)PyArray_DATA(PyList_GetItem(PyList_GetItem(db_, 0), 7));
	auto p_comment= (VarChar<23>*)PyArray_DATA(PyList_GetItem(PyList_GetItem(db_, 0), 8));
	auto p_NA= (VarChar<1>*)PyArray_DATA(PyList_GetItem(PyList_GetItem(db_, 0), 9));

	auto su_size = PyArray_Size(PyList_GetItem(PyList_GetItem(db_, 1), 0));
	db->su_dataset_size = su_size;
	auto s_suppkey = (long*)PyArray_DATA(PyList_GetItem(PyList_GetItem(db_, 1), 0));
	auto s_name= (VarChar<25>*)PyArray_DATA(PyList_GetItem(PyList_GetItem(db_, 1), 1));
	auto s_address= (VarChar<40>*)PyArray_DATA(PyList_GetItem(PyList_GetItem(db_, 1), 2));
	auto s_nationkey = (long*)PyArray_DATA(PyList_GetItem(PyList_GetItem(db_, 1), 3));
	auto s_phone= (VarChar<15>*)PyArray_DATA(PyList_GetItem(PyList_GetItem(db_, 1), 4));
	auto s_acctbal = (double*)PyArray_DATA(PyList_GetItem(PyList_GetItem(db_, 1), 5));
	auto s_comment= (VarChar<101>*)PyArray_DATA(PyList_GetItem(PyList_GetItem(db_, 1), 6));
	auto s_NA= (VarChar<1>*)PyArray_DATA(PyList_GetItem(PyList_GetItem(db_, 1), 7));

	auto li_size = PyArray_Size(PyList_GetItem(PyList_GetItem(db_, 2), 0));
	db->li_dataset_size = li_size;
	auto l_orderkey = (long*)PyArray_DATA(PyList_GetItem(PyList_GetItem(db_, 2), 0));
	auto l_partkey = (long*)PyArray_DATA(PyList_GetItem(PyList_GetItem(db_, 2), 1));
	auto l_suppkey = (long*)PyArray_DATA(PyList_GetItem(PyList_GetItem(db_, 2), 2));
	auto l_linenumber = (long*)PyArray_DATA(PyList_GetItem(PyList_GetItem(db_, 2), 3));
	auto l_quantity = (double*)PyArray_DATA(PyList_GetItem(PyList_GetItem(db_, 2), 4));
	auto l_extendedprice = (double*)PyArray_DATA(PyList_GetItem(PyList_GetItem(db_, 2), 5));
	auto l_discount = (double*)PyArray_DATA(PyList_GetItem(PyList_GetItem(db_, 2), 6));
	auto l_tax = (double*)PyArray_DATA(PyList_GetItem(PyList_GetItem(db_, 2), 7));
	auto l_returnflag= (VarChar<1>*)PyArray_DATA(PyList_GetItem(PyList_GetItem(db_, 2), 8));
	auto l_linestatus= (VarChar<1>*)PyArray_DATA(PyList_GetItem(PyList_GetItem(db_, 2), 9));
	auto l_shipdate = (long*)PyArray_DATA(PyList_GetItem(PyList_GetItem(db_, 2), 10));
	auto l_commitdate = (long*)PyArray_DATA(PyList_GetItem(PyList_GetItem(db_, 2), 11));
	auto l_receiptdate = (long*)PyArray_DATA(PyList_GetItem(PyList_GetItem(db_, 2), 12));
	auto l_shipinstruct= (VarChar<25>*)PyArray_DATA(PyList_GetItem(PyList_GetItem(db_, 2), 13));
	auto l_shipmode= (VarChar<10>*)PyArray_DATA(PyList_GetItem(PyList_GetItem(db_, 2), 14));
	auto l_comment= (VarChar<44>*)PyArray_DATA(PyList_GetItem(PyList_GetItem(db_, 2), 15));
	auto l_NA= (VarChar<1>*)PyArray_DATA(PyList_GetItem(PyList_GetItem(db_, 2), 16));

	auto ord_size = PyArray_Size(PyList_GetItem(PyList_GetItem(db_, 3), 0));
	db->ord_dataset_size = ord_size;
	auto o_orderkey = (long*)PyArray_DATA(PyList_GetItem(PyList_GetItem(db_, 3), 0));
	auto o_custkey = (long*)PyArray_DATA(PyList_GetItem(PyList_GetItem(db_, 3), 1));
	auto o_orderstatus= (VarChar<1>*)PyArray_DATA(PyList_GetItem(PyList_GetItem(db_, 3), 2));
	auto o_totalprice = (double*)PyArray_DATA(PyList_GetItem(PyList_GetItem(db_, 3), 3));
	auto o_orderdate = (long*)PyArray_DATA(PyList_GetItem(PyList_GetItem(db_, 3), 4));
	auto o_orderpriority= (VarChar<15>*)PyArray_DATA(PyList_GetItem(PyList_GetItem(db_, 3), 5));
	auto o_clerk= (VarChar<15>*)PyArray_DATA(PyList_GetItem(PyList_GetItem(db_, 3), 6));
	auto o_shippriority = (long*)PyArray_DATA(PyList_GetItem(PyList_GetItem(db_, 3), 7));
	auto o_comment= (VarChar<79>*)PyArray_DATA(PyList_GetItem(PyList_GetItem(db_, 3), 8));
	auto o_NA= (VarChar<1>*)PyArray_DATA(PyList_GetItem(PyList_GetItem(db_, 3), 9));

	auto cu_size = PyArray_Size(PyList_GetItem(PyList_GetItem(db_, 4), 0));
	db->cu_dataset_size = cu_size;
	auto c_custkey = (long*)PyArray_DATA(PyList_GetItem(PyList_GetItem(db_, 4), 0));
	auto c_name= (VarChar<25>*)PyArray_DATA(PyList_GetItem(PyList_GetItem(db_, 4), 1));
	auto c_address= (VarChar<40>*)PyArray_DATA(PyList_GetItem(PyList_GetItem(db_, 4), 2));
	auto c_nationkey = (long*)PyArray_DATA(PyList_GetItem(PyList_GetItem(db_, 4), 3));
	auto c_phone= (VarChar<15>*)PyArray_DATA(PyList_GetItem(PyList_GetItem(db_, 4), 4));
	auto c_acctbal = (double*)PyArray_DATA(PyList_GetItem(PyList_GetItem(db_, 4), 5));
	auto c_mktsegment= (VarChar<10>*)PyArray_DATA(PyList_GetItem(PyList_GetItem(db_, 4), 6));
	auto c_comment= (VarChar<117>*)PyArray_DATA(PyList_GetItem(PyList_GetItem(db_, 4), 7));
	auto c_NA= (VarChar<1>*)PyArray_DATA(PyList_GetItem(PyList_GetItem(db_, 4), 8));

	auto na_size = PyArray_Size(PyList_GetItem(PyList_GetItem(db_, 5), 0));
	db->na_dataset_size = na_size;
	auto n_nationkey = (long*)PyArray_DATA(PyList_GetItem(PyList_GetItem(db_, 5), 0));
	auto n_name= (VarChar<25>*)PyArray_DATA(PyList_GetItem(PyList_GetItem(db_, 5), 1));
	auto n_regionkey = (long*)PyArray_DATA(PyList_GetItem(PyList_GetItem(db_, 5), 2));
	auto n_comment= (VarChar<152>*)PyArray_DATA(PyList_GetItem(PyList_GetItem(db_, 5), 3));
	auto n_NA= (VarChar<1>*)PyArray_DATA(PyList_GetItem(PyList_GetItem(db_, 5), 4));

	auto re_size = PyArray_Size(PyList_GetItem(PyList_GetItem(db_, 6), 0));
	db->re_dataset_size = re_size;
	auto r_regionkey = (long*)PyArray_DATA(PyList_GetItem(PyList_GetItem(db_, 6), 0));
	auto r_name= (VarChar<25>*)PyArray_DATA(PyList_GetItem(PyList_GetItem(db_, 6), 1));
	auto r_comment= (VarChar<152>*)PyArray_DATA(PyList_GetItem(PyList_GetItem(db_, 6), 2));
	auto r_NA= (VarChar<1>*)PyArray_DATA(PyList_GetItem(PyList_GetItem(db_, 6), 3));



	
auto v666 = phmap::flat_hash_map<tuple<long,VarChar<25>,long,VarChar<152>,VarChar<1>,long>,bool>({});
tbb::enumerable_thread_specific<phmap::flat_hash_map<tuple<long,VarChar<25>,long,VarChar<152>,VarChar<1>,long>,bool>> v751;
auto v668 = phmap::flat_hash_map<tuple<long,VarChar<25>,long,VarChar<152>,VarChar<1>,long,VarChar<25>>,bool>({});
auto v670 = phmap::flat_hash_map<tuple<long,VarChar<25>,long,VarChar<152>,VarChar<1>,long,VarChar<25>,long>,bool>({});
auto v672 = phmap::flat_hash_map<tuple<long,VarChar<25>,long,VarChar<152>,VarChar<1>,long,VarChar<25>,long,VarChar<152>>,bool>({});
auto v674 = phmap::flat_hash_map<tuple<long,VarChar<55>,VarChar<25>,VarChar<10>,VarChar<25>,long,VarChar<10>,double,VarChar<23>,VarChar<1>>,bool>({});
tbb::enumerable_thread_specific<phmap::flat_hash_map<tuple<long,VarChar<55>,VarChar<25>,VarChar<10>,VarChar<25>,long,VarChar<10>,double,VarChar<23>,VarChar<1>>,bool>> v758;
auto v676 = phmap::flat_hash_map<tuple<long,VarChar<25>,VarChar<152>,VarChar<1>>,bool>({});
tbb::enumerable_thread_specific<phmap::flat_hash_map<tuple<long,VarChar<25>,VarChar<152>,VarChar<1>>,bool>> v765;
auto v678 = phmap::flat_hash_map<tuple<long,VarChar<25>,long,VarChar<152>,VarChar<1>,long>,bool>({});
tbb::enumerable_thread_specific<phmap::flat_hash_map<tuple<long,VarChar<25>,long,VarChar<152>,VarChar<1>,long>,bool>> v772;
auto v680 = phmap::flat_hash_map<tuple<long,VarChar<25>,long,VarChar<152>,VarChar<1>,long,VarChar<25>>,bool>({});
auto v682 = phmap::flat_hash_map<tuple<long,VarChar<25>,long,VarChar<152>,VarChar<1>,long,VarChar<25>,long>,bool>({});
auto v684 = phmap::flat_hash_map<tuple<long,VarChar<25>,long,VarChar<152>,VarChar<1>,long,VarChar<25>,long,VarChar<152>>,bool>({});
auto v686 = phmap::flat_hash_map<long,phmap::flat_hash_map<tuple<long,VarChar<25>,VarChar<152>,VarChar<1>>,bool>>({});
auto v690 = phmap::flat_hash_map<tuple<long,VarChar<25>,long,VarChar<152>,VarChar<1>,long,VarChar<25>,long,VarChar<152>,long,VarChar<25>,VarChar<152>,VarChar<1>>,bool>({});
auto v692 = phmap::flat_hash_map<long,phmap::flat_hash_map<tuple<long,VarChar<25>,long,VarChar<152>,VarChar<1>,long,VarChar<25>,long,VarChar<152>,long,VarChar<25>,VarChar<152>,VarChar<1>>,bool>>({});
auto v696 = phmap::flat_hash_map<tuple<long,VarChar<25>,VarChar<40>,long,VarChar<15>,double,VarChar<10>,VarChar<117>,VarChar<1>,long,VarChar<25>,long,VarChar<152>,VarChar<1>,long,VarChar<25>,long,VarChar<152>,long,VarChar<25>,VarChar<152>,VarChar<1>>,bool>({});
tbb::enumerable_thread_specific<phmap::flat_hash_map<tuple<long,VarChar<25>,VarChar<40>,long,VarChar<15>,double,VarChar<10>,VarChar<117>,VarChar<1>,long,VarChar<25>,long,VarChar<152>,VarChar<1>,long,VarChar<25>,long,VarChar<152>,long,VarChar<25>,VarChar<152>,VarChar<1>>,bool>> v779;
auto v698 = phmap::flat_hash_map<tuple<long,long,VarChar<1>,double,long,VarChar<15>,VarChar<15>,long,VarChar<79>,VarChar<1>>,bool>({});
tbb::enumerable_thread_specific<phmap::flat_hash_map<tuple<long,long,VarChar<1>,double,long,VarChar<15>,VarChar<15>,long,VarChar<79>,VarChar<1>>,bool>> v786;
auto v700 = phmap::flat_hash_map<long,phmap::flat_hash_map<tuple<long,VarChar<25>,VarChar<40>,long,VarChar<15>,double,VarChar<10>,VarChar<117>,VarChar<1>,long,VarChar<25>,long,VarChar<152>,VarChar<1>,long,VarChar<25>,long,VarChar<152>,long,VarChar<25>,VarChar<152>,VarChar<1>>,bool>>({});
auto v704 = phmap::flat_hash_map<tuple<long,long,VarChar<1>,double,long,VarChar<15>,VarChar<15>,long,VarChar<79>,VarChar<1>,long,VarChar<25>,VarChar<40>,long,VarChar<15>,double,VarChar<10>,VarChar<117>,VarChar<1>,long,VarChar<25>,long,VarChar<152>,VarChar<1>,long,VarChar<25>,long,VarChar<152>,long,VarChar<25>,VarChar<152>,VarChar<1>>,bool>({});
auto v706 = phmap::flat_hash_map<long,phmap::flat_hash_map<tuple<long,long,VarChar<1>,double,long,VarChar<15>,VarChar<15>,long,VarChar<79>,VarChar<1>,long,VarChar<25>,VarChar<40>,long,VarChar<15>,double,VarChar<10>,VarChar<117>,VarChar<1>,long,VarChar<25>,long,VarChar<152>,VarChar<1>,long,VarChar<25>,long,VarChar<152>,long,VarChar<25>,VarChar<152>,VarChar<1>>,bool>>({});
auto v710 = phmap::flat_hash_map<tuple<long,long,long,long,double,double,double,double,VarChar<1>,VarChar<1>,long,long,long,VarChar<25>,VarChar<10>,VarChar<44>,VarChar<1>,long,long,VarChar<1>,double,long,VarChar<15>,VarChar<15>,long,VarChar<79>,VarChar<1>,long,VarChar<25>,VarChar<40>,long,VarChar<15>,double,VarChar<10>,VarChar<117>,VarChar<1>,long,VarChar<25>,long,VarChar<152>,VarChar<1>,long,VarChar<25>,long,VarChar<152>,long,VarChar<25>,VarChar<152>,VarChar<1>>,bool>({});
tbb::enumerable_thread_specific<phmap::flat_hash_map<tuple<long,long,long,long,double,double,double,double,VarChar<1>,VarChar<1>,long,long,long,VarChar<25>,VarChar<10>,VarChar<44>,VarChar<1>,long,long,VarChar<1>,double,long,VarChar<15>,VarChar<15>,long,VarChar<79>,VarChar<1>,long,VarChar<25>,VarChar<40>,long,VarChar<15>,double,VarChar<10>,VarChar<117>,VarChar<1>,long,VarChar<25>,long,VarChar<152>,VarChar<1>,long,VarChar<25>,long,VarChar<152>,long,VarChar<25>,VarChar<152>,VarChar<1>>,bool>> v793;
auto v712 = phmap::flat_hash_map<long,phmap::flat_hash_map<tuple<long,VarChar<55>,VarChar<25>,VarChar<10>,VarChar<25>,long,VarChar<10>,double,VarChar<23>,VarChar<1>>,bool>>({});
auto v716 = phmap::flat_hash_map<tuple<long,long,long,long,double,double,double,double,VarChar<1>,VarChar<1>,long,long,long,VarChar<25>,VarChar<10>,VarChar<44>,VarChar<1>,long,long,VarChar<1>,double,long,VarChar<15>,VarChar<15>,long,VarChar<79>,VarChar<1>,long,VarChar<25>,VarChar<40>,long,VarChar<15>,double,VarChar<10>,VarChar<117>,VarChar<1>,long,VarChar<25>,long,VarChar<152>,VarChar<1>,long,VarChar<25>,long,VarChar<152>,long,VarChar<25>,VarChar<152>,VarChar<1>,long,VarChar<55>,VarChar<25>,VarChar<10>,VarChar<25>,long,VarChar<10>,double,VarChar<23>,VarChar<1>>,bool>({});
auto v718 = phmap::flat_hash_map<long,phmap::flat_hash_map<tuple<long,VarChar<25>,VarChar<40>,long,VarChar<15>,double,VarChar<101>,VarChar<1>>,bool>>({});
tbb::enumerable_thread_specific<phmap::flat_hash_map<long,phmap::flat_hash_map<tuple<long,VarChar<25>,VarChar<40>,long,VarChar<15>,double,VarChar<101>,VarChar<1>>,bool>>> v800;
auto v722 = phmap::flat_hash_map<tuple<long,long,long,long,double,double,double,double,VarChar<1>,VarChar<1>,long,long,long,VarChar<25>,VarChar<10>,VarChar<44>,VarChar<1>,long,long,VarChar<1>,double,long,VarChar<15>,VarChar<15>,long,VarChar<79>,VarChar<1>,long,VarChar<25>,VarChar<40>,long,VarChar<15>,double,VarChar<10>,VarChar<117>,VarChar<1>,long,VarChar<25>,long,VarChar<152>,VarChar<1>,long,VarChar<25>,long,VarChar<152>,long,VarChar<25>,VarChar<152>,VarChar<1>,long,VarChar<55>,VarChar<25>,VarChar<10>,VarChar<25>,long,VarChar<10>,double,VarChar<23>,VarChar<1>,long,VarChar<25>,VarChar<40>,long,VarChar<15>,double,VarChar<101>,VarChar<1>>,bool>({});
auto v724 = phmap::flat_hash_map<long,phmap::flat_hash_map<tuple<long,VarChar<25>,long,VarChar<152>,VarChar<1>,long,VarChar<25>,long,VarChar<152>>,bool>>({});
auto v728 = phmap::flat_hash_map<tuple<long,long,long,long,double,double,double,double,VarChar<1>,VarChar<1>,long,long,long,VarChar<25>,VarChar<10>,VarChar<44>,VarChar<1>,long,long,VarChar<1>,double,long,VarChar<15>,VarChar<15>,long,VarChar<79>,VarChar<1>,long,VarChar<25>,VarChar<40>,long,VarChar<15>,double,VarChar<10>,VarChar<117>,VarChar<1>,long,VarChar<25>,long,VarChar<152>,VarChar<1>,long,VarChar<25>,long,VarChar<152>,long,VarChar<25>,VarChar<152>,VarChar<1>,long,VarChar<55>,VarChar<25>,VarChar<10>,VarChar<25>,long,VarChar<10>,double,VarChar<23>,VarChar<1>,long,VarChar<25>,VarChar<40>,long,VarChar<15>,double,VarChar<101>,VarChar<1>,long,VarChar<25>,long,VarChar<152>,VarChar<1>,long,VarChar<25>,long,VarChar<152>>,bool>({});
auto v730 = phmap::flat_hash_map<tuple<long,long,long,long,double,double,double,double,VarChar<1>,VarChar<1>,long,long,long,VarChar<25>,VarChar<10>,VarChar<44>,VarChar<1>,long,long,VarChar<1>,double,long,VarChar<15>,VarChar<15>,long,VarChar<79>,VarChar<1>,long,VarChar<25>,VarChar<40>,long,VarChar<15>,double,VarChar<10>,VarChar<117>,VarChar<1>,long,VarChar<25>,long,VarChar<152>,VarChar<1>,long,VarChar<25>,long,VarChar<152>,long,VarChar<25>,VarChar<152>,VarChar<1>,long,VarChar<55>,VarChar<25>,VarChar<10>,VarChar<25>,long,VarChar<10>,double,VarChar<23>,VarChar<1>,long,VarChar<25>,VarChar<40>,long,VarChar<15>,double,VarChar<101>,VarChar<1>,long,VarChar<25>,long,VarChar<152>,VarChar<1>,long,VarChar<25>,long,VarChar<152>,long>,bool>({});
auto v732 = phmap::flat_hash_map<tuple<long,long,long,long,double,double,double,double,VarChar<1>,VarChar<1>,long,long,long,VarChar<25>,VarChar<10>,VarChar<44>,VarChar<1>,long,long,VarChar<1>,double,long,VarChar<15>,VarChar<15>,long,VarChar<79>,VarChar<1>,long,VarChar<25>,VarChar<40>,long,VarChar<15>,double,VarChar<10>,VarChar<117>,VarChar<1>,long,VarChar<25>,long,VarChar<152>,VarChar<1>,long,VarChar<25>,long,VarChar<152>,long,VarChar<25>,VarChar<152>,VarChar<1>,long,VarChar<55>,VarChar<25>,VarChar<10>,VarChar<25>,long,VarChar<10>,double,VarChar<23>,VarChar<1>,long,VarChar<25>,VarChar<40>,long,VarChar<15>,double,VarChar<101>,VarChar<1>,long,VarChar<25>,long,VarChar<152>,VarChar<1>,long,VarChar<25>,long,VarChar<152>,long,double>,bool>({});
auto v734 = phmap::flat_hash_map<tuple<long,long,long,long,double,double,double,double,VarChar<1>,VarChar<1>,long,long,long,VarChar<25>,VarChar<10>,VarChar<44>,VarChar<1>,long,long,VarChar<1>,double,long,VarChar<15>,VarChar<15>,long,VarChar<79>,VarChar<1>,long,VarChar<25>,VarChar<40>,long,VarChar<15>,double,VarChar<10>,VarChar<117>,VarChar<1>,long,VarChar<25>,long,VarChar<152>,VarChar<1>,long,VarChar<25>,long,VarChar<152>,long,VarChar<25>,VarChar<152>,VarChar<1>,long,VarChar<55>,VarChar<25>,VarChar<10>,VarChar<25>,long,VarChar<10>,double,VarChar<23>,VarChar<1>,long,VarChar<25>,VarChar<40>,long,VarChar<15>,double,VarChar<101>,VarChar<1>,long,VarChar<25>,long,VarChar<152>,VarChar<1>,long,VarChar<25>,long,VarChar<152>,long,double,VarChar<25>>,bool>({});
auto v736 = phmap::flat_hash_map<tuple<long,long,long,long,double,double,double,double,VarChar<1>,VarChar<1>,long,long,long,VarChar<25>,VarChar<10>,VarChar<44>,VarChar<1>,long,long,VarChar<1>,double,long,VarChar<15>,VarChar<15>,long,VarChar<79>,VarChar<1>,long,VarChar<25>,VarChar<40>,long,VarChar<15>,double,VarChar<10>,VarChar<117>,VarChar<1>,long,VarChar<25>,long,VarChar<152>,VarChar<1>,long,VarChar<25>,long,VarChar<152>,long,VarChar<25>,VarChar<152>,VarChar<1>,long,VarChar<55>,VarChar<25>,VarChar<10>,VarChar<25>,long,VarChar<10>,double,VarChar<23>,VarChar<1>,long,VarChar<25>,VarChar<40>,long,VarChar<15>,double,VarChar<101>,VarChar<1>,long,VarChar<25>,long,VarChar<152>,VarChar<1>,long,VarChar<25>,long,VarChar<152>,long,double,VarChar<25>,double>,bool>({});
auto v738 = phmap::flat_hash_map<tuple<long>,tuple<double,double>>({});
auto v740 = phmap::flat_hash_map<tuple<long,double,double>,bool>({});
auto v742 = phmap::flat_hash_map<tuple<long,double,double,double>,bool>({});
auto v744 = phmap::flat_hash_map<tuple<long,double,double,double>,phmap::flat_hash_map<tuple<long,double>,bool>>({});
auto v746 = phmap::flat_hash_map<tuple<long,double>,bool>({});	
	const auto& economyanodizedsteel = ConstantString("ECONOMY ANODIZED STEEL", 23);const auto& america = ConstantString("AMERICA", 8);const auto& brazil = ConstantString("BRAZIL", 7);

auto v747 = db->na_dataset_size; tbb::parallel_for(tbb::blocked_range<size_t>(0, v747), [&](const tbb::blocked_range<size_t>& v748){auto& v752=v751.local();for (size_t v665=v748.begin(), end=v748.end(); v665!=end; ++v665){v752[tuple_cat(make_tuple(n_nationkey[v665], n_name[v665], n_regionkey[v665], n_comment[v665], n_NA[v665]),move(make_tuple(n_nationkey[v665])))] += true;}});for (auto& local : v751)AddMap<phmap::flat_hash_map<tuple<long,VarChar<25>,long,VarChar<152>,VarChar<1>,long>,bool>,tuple<long,VarChar<25>,long,VarChar<152>,VarChar<1>,long>,bool>(v666, local);const auto& n2_0 = v666;

for (auto& v667 : n2_0){v668[tuple_cat((v667.first),move(make_tuple( /* n_name */get<1>((v667.first)))))] += (v667.second);}const auto& n2_1 = v668;

for (auto& v669 : n2_1){v670[tuple_cat((v669.first),move(make_tuple( /* n_regionkey */get<2>((v669.first)))))] += (v669.second);}const auto& n2_2 = v670;

for (auto& v671 : n2_2){v672[tuple_cat((v671.first),move(make_tuple( /* n_comment */get<3>((v671.first)))))] += (v671.second);}const auto& n2_supplier_part_region_n1_customer_orders_lineitem_build_pre_ops = v672;

auto v754 = db->pa_dataset_size; tbb::parallel_for(tbb::blocked_range<size_t>(0, v754), [&](const tbb::blocked_range<size_t>& v755){auto& v759=v758.local();for (size_t v673=v755.begin(), end=v755.end(); v673!=end; ++v673){if ((p_type[v673] == economyanodizedsteel)){v759[make_tuple(p_partkey[v673], p_name[v673], p_mfgr[v673], p_brand[v673], p_type[v673], p_size[v673], p_container[v673], p_retailprice[v673], p_comment[v673], p_NA[v673])] += true;};}});for (auto& local : v758)AddMap<phmap::flat_hash_map<tuple<long,VarChar<55>,VarChar<25>,VarChar<10>,VarChar<25>,long,VarChar<10>,double,VarChar<23>,VarChar<1>>,bool>,tuple<long,VarChar<55>,VarChar<25>,VarChar<10>,VarChar<25>,long,VarChar<10>,double,VarChar<23>,VarChar<1>>,bool>(v674, local);const auto& part_region_n1_customer_orders_lineitem_build_pre_ops = v674;

auto v761 = db->re_dataset_size; tbb::parallel_for(tbb::blocked_range<size_t>(0, v761), [&](const tbb::blocked_range<size_t>& v762){auto& v766=v765.local();for (size_t v675=v762.begin(), end=v762.end(); v675!=end; ++v675){if ((r_name[v675] == america)){v766[make_tuple(r_regionkey[v675], r_name[v675], r_comment[v675], r_NA[v675])] += true;};}});for (auto& local : v765)AddMap<phmap::flat_hash_map<tuple<long,VarChar<25>,VarChar<152>,VarChar<1>>,bool>,tuple<long,VarChar<25>,VarChar<152>,VarChar<1>>,bool>(v676, local);const auto& region_n1_build_pre_ops = v676;

auto v768 = db->na_dataset_size; tbb::parallel_for(tbb::blocked_range<size_t>(0, v768), [&](const tbb::blocked_range<size_t>& v769){auto& v773=v772.local();for (size_t v677=v769.begin(), end=v769.end(); v677!=end; ++v677){v773[tuple_cat(make_tuple(n_nationkey[v677], n_name[v677], n_regionkey[v677], n_comment[v677], n_NA[v677]),move(make_tuple(n_nationkey[v677])))] += true;}});for (auto& local : v772)AddMap<phmap::flat_hash_map<tuple<long,VarChar<25>,long,VarChar<152>,VarChar<1>,long>,bool>,tuple<long,VarChar<25>,long,VarChar<152>,VarChar<1>,long>,bool>(v678, local);const auto& n1_0 = v678;

for (auto& v679 : n1_0){v680[tuple_cat((v679.first),move(make_tuple( /* n_name */get<1>((v679.first)))))] += (v679.second);}const auto& n1_1 = v680;

for (auto& v681 : n1_1){v682[tuple_cat((v681.first),move(make_tuple( /* n_regionkey */get<2>((v681.first)))))] += (v681.second);}const auto& n1_2 = v682;

for (auto& v683 : n1_2){v684[tuple_cat((v683.first),move(make_tuple( /* n_comment */get<3>((v683.first)))))] += (v683.second);}const auto& region_n1_probe_pre_ops = v684;

for (auto& v685 : region_n1_build_pre_ops){v686[ /* r_regionkey */get<0>((v685.first))] += phmap::flat_hash_map<tuple<long,VarChar<25>,VarChar<152>,VarChar<1>>,bool>({{(v685.first), (v685.second)}});}const auto& region_n1_build_nest_dict = v686;

for (auto& v687 : region_n1_probe_pre_ops){if (((region_n1_build_nest_dict).contains( /* n1_regionkey */get<7>((v687.first))))){for (auto& v688 : (region_n1_build_nest_dict).at( /* n1_regionkey */get<7>((v687.first)))){v690[tuple_cat((v687.first),move((v688.first)))] += true;};};}const auto& region_n1_customer_build_pre_ops = v690;

for (auto& v691 : region_n1_customer_build_pre_ops){v692[ /* n1_nationkey */get<5>((v691.first))] += phmap::flat_hash_map<tuple<long,VarChar<25>,long,VarChar<152>,VarChar<1>,long,VarChar<25>,long,VarChar<152>,long,VarChar<25>,VarChar<152>,VarChar<1>>,bool>({{(v691.first), (v691.second)}});}const auto& region_n1_customer_build_nest_dict = v692;

auto v775 = db->cu_dataset_size; tbb::parallel_for(tbb::blocked_range<size_t>(0, v775), [&](const tbb::blocked_range<size_t>& v776){auto& v780=v779.local();for (size_t v693=v776.begin(), end=v776.end(); v693!=end; ++v693){if (((region_n1_customer_build_nest_dict).contains(c_nationkey[v693]))){for (auto& v694 : (region_n1_customer_build_nest_dict).at(c_nationkey[v693])){v780[tuple_cat(make_tuple(c_custkey[v693], c_name[v693], c_address[v693], c_nationkey[v693], c_phone[v693], c_acctbal[v693], c_mktsegment[v693], c_comment[v693], c_NA[v693]),move(make_tuple(n_nationkey[v694], n_name[v694], n_regionkey[v694], n_comment[v694], n_NA[v694], n1_nationkey[v694], n1_name[v694], n1_regionkey[v694], n1_comment[v694], r_regionkey[v694], r_name[v694], r_comment[v694], r_NA[v694])))] += true;};};}});for (auto& local : v779)AddMap<phmap::flat_hash_map<tuple<long,VarChar<25>,VarChar<40>,long,VarChar<15>,double,VarChar<10>,VarChar<117>,VarChar<1>,long,VarChar<25>,long,VarChar<152>,VarChar<1>,long,VarChar<25>,long,VarChar<152>,long,VarChar<25>,VarChar<152>,VarChar<1>>,bool>,tuple<long,VarChar<25>,VarChar<40>,long,VarChar<15>,double,VarChar<10>,VarChar<117>,VarChar<1>,long,VarChar<25>,long,VarChar<152>,VarChar<1>,long,VarChar<25>,long,VarChar<152>,long,VarChar<25>,VarChar<152>,VarChar<1>>,bool>(v696, local);const auto& region_n1_customer_orders_build_pre_ops = v696;

auto v782 = db->ord_dataset_size; tbb::parallel_for(tbb::blocked_range<size_t>(0, v782), [&](const tbb::blocked_range<size_t>& v783){auto& v787=v786.local();for (size_t v697=v783.begin(), end=v783.end(); v697!=end; ++v697){if (((o_orderdate[v697] >= (long)19950101) && (o_orderdate[v697] <= (long)19961231))){v787[make_tuple(o_orderkey[v697], o_custkey[v697], o_orderstatus[v697], o_totalprice[v697], o_orderdate[v697], o_orderpriority[v697], o_clerk[v697], o_shippriority[v697], o_comment[v697], o_NA[v697])] += true;};}});for (auto& local : v786)AddMap<phmap::flat_hash_map<tuple<long,long,VarChar<1>,double,long,VarChar<15>,VarChar<15>,long,VarChar<79>,VarChar<1>>,bool>,tuple<long,long,VarChar<1>,double,long,VarChar<15>,VarChar<15>,long,VarChar<79>,VarChar<1>>,bool>(v698, local);const auto& region_n1_customer_orders_probe_pre_ops = v698;

for (auto& v699 : region_n1_customer_orders_build_pre_ops){v700[ /* c_custkey */get<0>((v699.first))] += phmap::flat_hash_map<tuple<long,VarChar<25>,VarChar<40>,long,VarChar<15>,double,VarChar<10>,VarChar<117>,VarChar<1>,long,VarChar<25>,long,VarChar<152>,VarChar<1>,long,VarChar<25>,long,VarChar<152>,long,VarChar<25>,VarChar<152>,VarChar<1>>,bool>({{(v699.first), (v699.second)}});}const auto& region_n1_customer_orders_build_nest_dict = v700;

for (auto& v701 : region_n1_customer_orders_probe_pre_ops){if (((region_n1_customer_orders_build_nest_dict).contains( /* o_custkey */get<1>((v701.first))))){for (auto& v702 : (region_n1_customer_orders_build_nest_dict).at( /* o_custkey */get<1>((v701.first)))){v704[tuple_cat((v701.first),move((v702.first)))] += true;};};}const auto& region_n1_customer_orders_lineitem_build_pre_ops = v704;

for (auto& v705 : region_n1_customer_orders_lineitem_build_pre_ops){v706[ /* o_orderkey */get<0>((v705.first))] += phmap::flat_hash_map<tuple<long,long,VarChar<1>,double,long,VarChar<15>,VarChar<15>,long,VarChar<79>,VarChar<1>,long,VarChar<25>,VarChar<40>,long,VarChar<15>,double,VarChar<10>,VarChar<117>,VarChar<1>,long,VarChar<25>,long,VarChar<152>,VarChar<1>,long,VarChar<25>,long,VarChar<152>,long,VarChar<25>,VarChar<152>,VarChar<1>>,bool>({{(v705.first), (v705.second)}});}const auto& region_n1_customer_orders_lineitem_build_nest_dict = v706;

auto v789 = db->li_dataset_size; tbb::parallel_for(tbb::blocked_range<size_t>(0, v789), [&](const tbb::blocked_range<size_t>& v790){auto& v794=v793.local();for (size_t v707=v790.begin(), end=v790.end(); v707!=end; ++v707){if (((region_n1_customer_orders_lineitem_build_nest_dict).contains(l_orderkey[v707]))){for (auto& v708 : (region_n1_customer_orders_lineitem_build_nest_dict).at(l_orderkey[v707])){v794[tuple_cat(make_tuple(l_orderkey[v707], l_partkey[v707], l_suppkey[v707], l_linenumber[v707], l_quantity[v707], l_extendedprice[v707], l_discount[v707], l_tax[v707], l_returnflag[v707], l_linestatus[v707], l_shipdate[v707], l_commitdate[v707], l_receiptdate[v707], l_shipinstruct[v707], l_shipmode[v707], l_comment[v707], l_NA[v707]),move(make_tuple(o_orderkey[v708], o_custkey[v708], o_orderstatus[v708], o_totalprice[v708], o_orderdate[v708], o_orderpriority[v708], o_clerk[v708], o_shippriority[v708], o_comment[v708], o_NA[v708], c_custkey[v708], c_name[v708], c_address[v708], c_nationkey[v708], c_phone[v708], c_acctbal[v708], c_mktsegment[v708], c_comment[v708], c_NA[v708], n_nationkey[v708], n_name[v708], n_regionkey[v708], n_comment[v708], n_NA[v708], n1_nationkey[v708], n1_name[v708], n1_regionkey[v708], n1_comment[v708], r_regionkey[v708], r_name[v708], r_comment[v708], r_NA[v708])))] += true;};};}});for (auto& local : v793)AddMap<phmap::flat_hash_map<tuple<long,long,long,long,double,double,double,double,VarChar<1>,VarChar<1>,long,long,long,VarChar<25>,VarChar<10>,VarChar<44>,VarChar<1>,long,long,VarChar<1>,double,long,VarChar<15>,VarChar<15>,long,VarChar<79>,VarChar<1>,long,VarChar<25>,VarChar<40>,long,VarChar<15>,double,VarChar<10>,VarChar<117>,VarChar<1>,long,VarChar<25>,long,VarChar<152>,VarChar<1>,long,VarChar<25>,long,VarChar<152>,long,VarChar<25>,VarChar<152>,VarChar<1>>,bool>,tuple<long,long,long,long,double,double,double,double,VarChar<1>,VarChar<1>,long,long,long,VarChar<25>,VarChar<10>,VarChar<44>,VarChar<1>,long,long,VarChar<1>,double,long,VarChar<15>,VarChar<15>,long,VarChar<79>,VarChar<1>,long,VarChar<25>,VarChar<40>,long,VarChar<15>,double,VarChar<10>,VarChar<117>,VarChar<1>,long,VarChar<25>,long,VarChar<152>,VarChar<1>,long,VarChar<25>,long,VarChar<152>,long,VarChar<25>,VarChar<152>,VarChar<1>>,bool>(v710, local);const auto& part_region_n1_customer_orders_lineitem_probe_pre_ops = v710;

for (auto& v711 : part_region_n1_customer_orders_lineitem_build_pre_ops){v712[ /* p_partkey */get<0>((v711.first))] += phmap::flat_hash_map<tuple<long,VarChar<55>,VarChar<25>,VarChar<10>,VarChar<25>,long,VarChar<10>,double,VarChar<23>,VarChar<1>>,bool>({{(v711.first), (v711.second)}});}const auto& part_region_n1_customer_orders_lineitem_build_nest_dict = v712;

for (auto& v713 : part_region_n1_customer_orders_lineitem_probe_pre_ops){if (((part_region_n1_customer_orders_lineitem_build_nest_dict).contains( /* l_partkey */get<1>((v713.first))))){for (auto& v714 : (part_region_n1_customer_orders_lineitem_build_nest_dict).at( /* l_partkey */get<1>((v713.first)))){v716[tuple_cat((v713.first),move((v714.first)))] += true;};};}const auto& supplier_part_region_n1_customer_orders_lineitem_probe_pre_ops = v716;

auto v796 = db->su_dataset_size; tbb::parallel_for(tbb::blocked_range<size_t>(0, v796), [&](const tbb::blocked_range<size_t>& v797){auto& v801=v800.local();for (size_t v717=v797.begin(), end=v797.end(); v717!=end; ++v717){v801[s_suppkey[v717]] += phmap::flat_hash_map<tuple<long,VarChar<25>,VarChar<40>,long,VarChar<15>,double,VarChar<101>,VarChar<1>>,bool>({{make_tuple(s_suppkey[v717], s_name[v717], s_address[v717], s_nationkey[v717], s_phone[v717], s_acctbal[v717], s_comment[v717], s_NA[v717]), true}});}});for (auto& local : v800)AddMap<phmap::flat_hash_map<long,phmap::flat_hash_map<tuple<long,VarChar<25>,VarChar<40>,long,VarChar<15>,double,VarChar<101>,VarChar<1>>,bool>>,long,phmap::flat_hash_map<tuple<long,VarChar<25>,VarChar<40>,long,VarChar<15>,double,VarChar<101>,VarChar<1>>,bool>>(v718, local);const auto& supplier_part_region_n1_customer_orders_lineitem_build_nest_dict = v718;

for (auto& v719 : supplier_part_region_n1_customer_orders_lineitem_probe_pre_ops){if (((supplier_part_region_n1_customer_orders_lineitem_build_nest_dict).contains( /* l_suppkey */get<2>((v719.first))))){for (auto& v720 : (supplier_part_region_n1_customer_orders_lineitem_build_nest_dict).at( /* l_suppkey */get<2>((v719.first)))){v722[tuple_cat((v719.first),move((v720.first)))] += true;};};}const auto& n2_supplier_part_region_n1_customer_orders_lineitem_probe_pre_ops = v722;

for (auto& v723 : n2_supplier_part_region_n1_customer_orders_lineitem_build_pre_ops){v724[ /* n2_nationkey */get<5>((v723.first))] += phmap::flat_hash_map<tuple<long,VarChar<25>,long,VarChar<152>,VarChar<1>,long,VarChar<25>,long,VarChar<152>>,bool>({{(v723.first), (v723.second)}});}const auto& n2_supplier_part_region_n1_customer_orders_lineitem_build_nest_dict = v724;

for (auto& v725 : n2_supplier_part_region_n1_customer_orders_lineitem_probe_pre_ops){if (((n2_supplier_part_region_n1_customer_orders_lineitem_build_nest_dict).contains( /* s_nationkey */get<62>((v725.first))))){for (auto& v726 : (n2_supplier_part_region_n1_customer_orders_lineitem_build_nest_dict).at( /* s_nationkey */get<62>((v725.first)))){v728[tuple_cat((v725.first),move((v726.first)))] += true;};};}const auto& n2_supplier_part_region_n1_customer_orders_lineitem_0 = v728;

for (auto& v729 : n2_supplier_part_region_n1_customer_orders_lineitem_0){v730[tuple_cat((v729.first),move(make_tuple((( /* o_orderdate */get<21>((v729.first)))/10000))))] += (v729.second);}const auto& n2_supplier_part_region_n1_customer_orders_lineitem_1 = v730;

for (auto& v731 : n2_supplier_part_region_n1_customer_orders_lineitem_1){v732[tuple_cat((v731.first),move(make_tuple(( /* l_extendedprice */get<5>((v731.first)) * (1.0 -  /* l_discount */get<6>((v731.first)))))))] += (v731.second);}const auto& n2_supplier_part_region_n1_customer_orders_lineitem_2 = v732;

for (auto& v733 : n2_supplier_part_region_n1_customer_orders_lineitem_2){v734[tuple_cat((v733.first),move(make_tuple( /* n2_name */get<73>((v733.first)))))] += (v733.second);}const auto& n2_supplier_part_region_n1_customer_orders_lineitem_3 = v734;

for (auto& v735 : n2_supplier_part_region_n1_customer_orders_lineitem_3){v736[tuple_cat((v735.first),move(make_tuple((( /* nation */get<78>((v735.first)) == brazil)) ? ( /* volume */get<77>((v735.first))) : (0.0))))] += (v735.second);}const auto& n2_supplier_part_region_n1_customer_orders_lineitem_4 = v736;

for (auto& v737 : n2_supplier_part_region_n1_customer_orders_lineitem_4){v738[make_tuple( /* o_year */get<76>((v737.first)))] += make_tuple( /* volume_A */get<79>((v737.first)), /* volume */get<77>((v737.first)));}const auto& n2_supplier_part_region_n1_customer_orders_lineitem_5 = v738;

for (auto& v739 : n2_supplier_part_region_n1_customer_orders_lineitem_5){v740[tuple_cat((v739.first),move((v739.second)))] += true;}const auto& n2_supplier_part_region_n1_customer_orders_lineitem_6 = v740;

for (auto& v741 : n2_supplier_part_region_n1_customer_orders_lineitem_6){v742[tuple_cat((v741.first),move(make_tuple(( /* A */get<1>((v741.first)) /  /* B */get<2>((v741.first))))))] += (v741.second);}const auto& n2_supplier_part_region_n1_customer_orders_lineitem_7 = v742;

for (auto& v743 : n2_supplier_part_region_n1_customer_orders_lineitem_7){v744[(v743.first)] += phmap::flat_hash_map<tuple<long,double>,bool>({{make_tuple( /* o_year */get<0>((v743.first)), /* mkt_share */get<3>((v743.first))), true}});}const auto& n2_supplier_part_region_n1_customer_orders_lineitem_8 = v744;

for (auto& v745 : n2_supplier_part_region_n1_customer_orders_lineitem_8){(v745.second);}const auto& results = v746;const auto& out = results;

    FastDict_if_b* result = (FastDict_if_b*)PyObject_CallObject(PyObject_CallObject(PyObject_GetAttrString(PyImport_Import(PyUnicode_FromString("test_unopt_fastdict_compiled")), (char*)"new_FastDict_if_b"), nullptr), nullptr);
    *(result->dict) = out;

    return (PyObject*)PyObject_CallObject(PyObject_GetAttrString(PyImport_Import(PyUnicode_FromString("sdqlpy.fastd")), "fastd"), Py_BuildValue("(OO)", result, PyUnicode_FromString("test_unopt")));

}

static PyObject * q9(PyObject *self, PyObject* args)
{
	

    PyObject * db_;
    if (!PyArg_ParseTuple(args, "O", &db_)){
        PyErr_SetString(PyExc_ValueError,"Error while parsing the sum inputs.");
        return NULL;
    }
    
    
    DB dbobj;
    DB* db = &dbobj;
    
const static int numpy_initialized =  init_numpy();


	auto li_size = PyArray_Size(PyList_GetItem(PyList_GetItem(db_, 0), 0));
	db->li_dataset_size = li_size;
	auto l_orderkey = (long*)PyArray_DATA(PyList_GetItem(PyList_GetItem(db_, 0), 0));
	auto l_partkey = (long*)PyArray_DATA(PyList_GetItem(PyList_GetItem(db_, 0), 1));
	auto l_suppkey = (long*)PyArray_DATA(PyList_GetItem(PyList_GetItem(db_, 0), 2));
	auto l_linenumber = (long*)PyArray_DATA(PyList_GetItem(PyList_GetItem(db_, 0), 3));
	auto l_quantity = (double*)PyArray_DATA(PyList_GetItem(PyList_GetItem(db_, 0), 4));
	auto l_extendedprice = (double*)PyArray_DATA(PyList_GetItem(PyList_GetItem(db_, 0), 5));
	auto l_discount = (double*)PyArray_DATA(PyList_GetItem(PyList_GetItem(db_, 0), 6));
	auto l_tax = (double*)PyArray_DATA(PyList_GetItem(PyList_GetItem(db_, 0), 7));
	auto l_returnflag= (VarChar<1>*)PyArray_DATA(PyList_GetItem(PyList_GetItem(db_, 0), 8));
	auto l_linestatus= (VarChar<1>*)PyArray_DATA(PyList_GetItem(PyList_GetItem(db_, 0), 9));
	auto l_shipdate = (long*)PyArray_DATA(PyList_GetItem(PyList_GetItem(db_, 0), 10));
	auto l_commitdate = (long*)PyArray_DATA(PyList_GetItem(PyList_GetItem(db_, 0), 11));
	auto l_receiptdate = (long*)PyArray_DATA(PyList_GetItem(PyList_GetItem(db_, 0), 12));
	auto l_shipinstruct= (VarChar<25>*)PyArray_DATA(PyList_GetItem(PyList_GetItem(db_, 0), 13));
	auto l_shipmode= (VarChar<10>*)PyArray_DATA(PyList_GetItem(PyList_GetItem(db_, 0), 14));
	auto l_comment= (VarChar<44>*)PyArray_DATA(PyList_GetItem(PyList_GetItem(db_, 0), 15));
	auto l_NA= (VarChar<1>*)PyArray_DATA(PyList_GetItem(PyList_GetItem(db_, 0), 16));

	auto ord_size = PyArray_Size(PyList_GetItem(PyList_GetItem(db_, 1), 0));
	db->ord_dataset_size = ord_size;
	auto o_orderkey = (long*)PyArray_DATA(PyList_GetItem(PyList_GetItem(db_, 1), 0));
	auto o_custkey = (long*)PyArray_DATA(PyList_GetItem(PyList_GetItem(db_, 1), 1));
	auto o_orderstatus= (VarChar<1>*)PyArray_DATA(PyList_GetItem(PyList_GetItem(db_, 1), 2));
	auto o_totalprice = (double*)PyArray_DATA(PyList_GetItem(PyList_GetItem(db_, 1), 3));
	auto o_orderdate = (long*)PyArray_DATA(PyList_GetItem(PyList_GetItem(db_, 1), 4));
	auto o_orderpriority= (VarChar<15>*)PyArray_DATA(PyList_GetItem(PyList_GetItem(db_, 1), 5));
	auto o_clerk= (VarChar<15>*)PyArray_DATA(PyList_GetItem(PyList_GetItem(db_, 1), 6));
	auto o_shippriority = (long*)PyArray_DATA(PyList_GetItem(PyList_GetItem(db_, 1), 7));
	auto o_comment= (VarChar<79>*)PyArray_DATA(PyList_GetItem(PyList_GetItem(db_, 1), 8));
	auto o_NA= (VarChar<1>*)PyArray_DATA(PyList_GetItem(PyList_GetItem(db_, 1), 9));

	auto na_size = PyArray_Size(PyList_GetItem(PyList_GetItem(db_, 2), 0));
	db->na_dataset_size = na_size;
	auto n_nationkey = (long*)PyArray_DATA(PyList_GetItem(PyList_GetItem(db_, 2), 0));
	auto n_name= (VarChar<25>*)PyArray_DATA(PyList_GetItem(PyList_GetItem(db_, 2), 1));
	auto n_regionkey = (long*)PyArray_DATA(PyList_GetItem(PyList_GetItem(db_, 2), 2));
	auto n_comment= (VarChar<152>*)PyArray_DATA(PyList_GetItem(PyList_GetItem(db_, 2), 3));
	auto n_NA= (VarChar<1>*)PyArray_DATA(PyList_GetItem(PyList_GetItem(db_, 2), 4));

	auto su_size = PyArray_Size(PyList_GetItem(PyList_GetItem(db_, 3), 0));
	db->su_dataset_size = su_size;
	auto s_suppkey = (long*)PyArray_DATA(PyList_GetItem(PyList_GetItem(db_, 3), 0));
	auto s_name= (VarChar<25>*)PyArray_DATA(PyList_GetItem(PyList_GetItem(db_, 3), 1));
	auto s_address= (VarChar<40>*)PyArray_DATA(PyList_GetItem(PyList_GetItem(db_, 3), 2));
	auto s_nationkey = (long*)PyArray_DATA(PyList_GetItem(PyList_GetItem(db_, 3), 3));
	auto s_phone= (VarChar<15>*)PyArray_DATA(PyList_GetItem(PyList_GetItem(db_, 3), 4));
	auto s_acctbal = (double*)PyArray_DATA(PyList_GetItem(PyList_GetItem(db_, 3), 5));
	auto s_comment= (VarChar<101>*)PyArray_DATA(PyList_GetItem(PyList_GetItem(db_, 3), 6));
	auto s_NA= (VarChar<1>*)PyArray_DATA(PyList_GetItem(PyList_GetItem(db_, 3), 7));

	auto pa_size = PyArray_Size(PyList_GetItem(PyList_GetItem(db_, 4), 0));
	db->pa_dataset_size = pa_size;
	auto p_partkey = (long*)PyArray_DATA(PyList_GetItem(PyList_GetItem(db_, 4), 0));
	auto p_name= (VarChar<55>*)PyArray_DATA(PyList_GetItem(PyList_GetItem(db_, 4), 1));
	auto p_mfgr= (VarChar<25>*)PyArray_DATA(PyList_GetItem(PyList_GetItem(db_, 4), 2));
	auto p_brand= (VarChar<10>*)PyArray_DATA(PyList_GetItem(PyList_GetItem(db_, 4), 3));
	auto p_type= (VarChar<25>*)PyArray_DATA(PyList_GetItem(PyList_GetItem(db_, 4), 4));
	auto p_size = (long*)PyArray_DATA(PyList_GetItem(PyList_GetItem(db_, 4), 5));
	auto p_container= (VarChar<10>*)PyArray_DATA(PyList_GetItem(PyList_GetItem(db_, 4), 6));
	auto p_retailprice = (double*)PyArray_DATA(PyList_GetItem(PyList_GetItem(db_, 4), 7));
	auto p_comment= (VarChar<23>*)PyArray_DATA(PyList_GetItem(PyList_GetItem(db_, 4), 8));
	auto p_NA= (VarChar<1>*)PyArray_DATA(PyList_GetItem(PyList_GetItem(db_, 4), 9));

	auto ps_size = PyArray_Size(PyList_GetItem(PyList_GetItem(db_, 5), 0));
	db->ps_dataset_size = ps_size;
	auto ps_partkey = (long*)PyArray_DATA(PyList_GetItem(PyList_GetItem(db_, 5), 0));
	auto ps_suppkey = (long*)PyArray_DATA(PyList_GetItem(PyList_GetItem(db_, 5), 1));
	auto ps_availqty = (double*)PyArray_DATA(PyList_GetItem(PyList_GetItem(db_, 5), 2));
	auto ps_supplycost = (double*)PyArray_DATA(PyList_GetItem(PyList_GetItem(db_, 5), 3));
	auto ps_comment= (VarChar<199>*)PyArray_DATA(PyList_GetItem(PyList_GetItem(db_, 5), 4));
	auto ps_NA= (VarChar<1>*)PyArray_DATA(PyList_GetItem(PyList_GetItem(db_, 5), 5));



	
auto v846 = phmap::flat_hash_map<long,phmap::flat_hash_map<tuple<long,VarChar<25>,long,VarChar<152>,VarChar<1>>,bool>>({});
tbb::enumerable_thread_specific<phmap::flat_hash_map<long,phmap::flat_hash_map<tuple<long,VarChar<25>,long,VarChar<152>,VarChar<1>>,bool>>> v891;
auto v850 = phmap::flat_hash_map<tuple<long,VarChar<25>,VarChar<40>,long,VarChar<15>,double,VarChar<101>,VarChar<1>,long,VarChar<25>,long,VarChar<152>,VarChar<1>>,bool>({});
tbb::enumerable_thread_specific<phmap::flat_hash_map<tuple<long,VarChar<25>,VarChar<40>,long,VarChar<15>,double,VarChar<101>,VarChar<1>,long,VarChar<25>,long,VarChar<152>,VarChar<1>>,bool>> v898;
auto v852 = phmap::flat_hash_map<tuple<long,VarChar<55>,VarChar<25>,VarChar<10>,VarChar<25>,long,VarChar<10>,double,VarChar<23>,VarChar<1>>,bool>({});
tbb::enumerable_thread_specific<phmap::flat_hash_map<tuple<long,VarChar<55>,VarChar<25>,VarChar<10>,VarChar<25>,long,VarChar<10>,double,VarChar<23>,VarChar<1>>,bool>> v905;
auto v854 = phmap::flat_hash_map<long,phmap::flat_hash_map<tuple<long,VarChar<55>,VarChar<25>,VarChar<10>,VarChar<25>,long,VarChar<10>,double,VarChar<23>,VarChar<1>>,bool>>({});
auto v858 = phmap::flat_hash_map<tuple<long,long,double,double,VarChar<199>,VarChar<1>,long,VarChar<55>,VarChar<25>,VarChar<10>,VarChar<25>,long,VarChar<10>,double,VarChar<23>,VarChar<1>>,bool>({});
tbb::enumerable_thread_specific<phmap::flat_hash_map<tuple<long,long,double,double,VarChar<199>,VarChar<1>,long,VarChar<55>,VarChar<25>,VarChar<10>,VarChar<25>,long,VarChar<10>,double,VarChar<23>,VarChar<1>>,bool>> v912;
auto v860 = phmap::flat_hash_map<long,phmap::flat_hash_map<tuple<long,VarChar<25>,VarChar<40>,long,VarChar<15>,double,VarChar<101>,VarChar<1>,long,VarChar<25>,long,VarChar<152>,VarChar<1>>,bool>>({});
auto v864 = phmap::flat_hash_map<tuple<long,long,double,double,VarChar<199>,VarChar<1>,long,VarChar<55>,VarChar<25>,VarChar<10>,VarChar<25>,long,VarChar<10>,double,VarChar<23>,VarChar<1>,long,VarChar<25>,VarChar<40>,long,VarChar<15>,double,VarChar<101>,VarChar<1>,long,VarChar<25>,long,VarChar<152>,VarChar<1>>,bool>({});
auto v866 = phmap::flat_hash_map<long,phmap::flat_hash_map<tuple<long,long,VarChar<1>,double,long,VarChar<15>,VarChar<15>,long,VarChar<79>,VarChar<1>>,bool>>({});
tbb::enumerable_thread_specific<phmap::flat_hash_map<long,phmap::flat_hash_map<tuple<long,long,VarChar<1>,double,long,VarChar<15>,VarChar<15>,long,VarChar<79>,VarChar<1>>,bool>>> v919;
auto v870 = phmap::flat_hash_map<tuple<long,long,long,long,double,double,double,double,VarChar<1>,VarChar<1>,long,long,long,VarChar<25>,VarChar<10>,VarChar<44>,VarChar<1>,long,long,VarChar<1>,double,long,VarChar<15>,VarChar<15>,long,VarChar<79>,VarChar<1>>,bool>({});
tbb::enumerable_thread_specific<phmap::flat_hash_map<tuple<long,long,long,long,double,double,double,double,VarChar<1>,VarChar<1>,long,long,long,VarChar<25>,VarChar<10>,VarChar<44>,VarChar<1>,long,long,VarChar<1>,double,long,VarChar<15>,VarChar<15>,long,VarChar<79>,VarChar<1>>,bool>> v926;
auto v872 = phmap::flat_hash_map<tuple<long,long>,phmap::flat_hash_map<tuple<long,long,double,double,VarChar<199>,VarChar<1>,long,VarChar<55>,VarChar<25>,VarChar<10>,VarChar<25>,long,VarChar<10>,double,VarChar<23>,VarChar<1>,long,VarChar<25>,VarChar<40>,long,VarChar<15>,double,VarChar<101>,VarChar<1>,long,VarChar<25>,long,VarChar<152>,VarChar<1>>,bool>>({});
auto v876 = phmap::flat_hash_map<tuple<long,long,long,long,double,double,double,double,VarChar<1>,VarChar<1>,long,long,long,VarChar<25>,VarChar<10>,VarChar<44>,VarChar<1>,long,long,VarChar<1>,double,long,VarChar<15>,VarChar<15>,long,VarChar<79>,VarChar<1>,long,long,double,double,VarChar<199>,VarChar<1>,long,VarChar<55>,VarChar<25>,VarChar<10>,VarChar<25>,long,VarChar<10>,double,VarChar<23>,VarChar<1>,long,VarChar<25>,VarChar<40>,long,VarChar<15>,double,VarChar<101>,VarChar<1>,long,VarChar<25>,long,VarChar<152>,VarChar<1>>,bool>({});
auto v878 = phmap::flat_hash_map<tuple<long,long,long,long,double,double,double,double,VarChar<1>,VarChar<1>,long,long,long,VarChar<25>,VarChar<10>,VarChar<44>,VarChar<1>,long,long,VarChar<1>,double,long,VarChar<15>,VarChar<15>,long,VarChar<79>,VarChar<1>,long,long,double,double,VarChar<199>,VarChar<1>,long,VarChar<55>,VarChar<25>,VarChar<10>,VarChar<25>,long,VarChar<10>,double,VarChar<23>,VarChar<1>,long,VarChar<25>,VarChar<40>,long,VarChar<15>,double,VarChar<101>,VarChar<1>,long,VarChar<25>,long,VarChar<152>,VarChar<1>,VarChar<25>>,bool>({});
auto v880 = phmap::flat_hash_map<tuple<long,long,long,long,double,double,double,double,VarChar<1>,VarChar<1>,long,long,long,VarChar<25>,VarChar<10>,VarChar<44>,VarChar<1>,long,long,VarChar<1>,double,long,VarChar<15>,VarChar<15>,long,VarChar<79>,VarChar<1>,long,long,double,double,VarChar<199>,VarChar<1>,long,VarChar<55>,VarChar<25>,VarChar<10>,VarChar<25>,long,VarChar<10>,double,VarChar<23>,VarChar<1>,long,VarChar<25>,VarChar<40>,long,VarChar<15>,double,VarChar<101>,VarChar<1>,long,VarChar<25>,long,VarChar<152>,VarChar<1>,VarChar<25>,long>,bool>({});
auto v882 = phmap::flat_hash_map<tuple<long,long,long,long,double,double,double,double,VarChar<1>,VarChar<1>,long,long,long,VarChar<25>,VarChar<10>,VarChar<44>,VarChar<1>,long,long,VarChar<1>,double,long,VarChar<15>,VarChar<15>,long,VarChar<79>,VarChar<1>,long,long,double,double,VarChar<199>,VarChar<1>,long,VarChar<55>,VarChar<25>,VarChar<10>,VarChar<25>,long,VarChar<10>,double,VarChar<23>,VarChar<1>,long,VarChar<25>,VarChar<40>,long,VarChar<15>,double,VarChar<101>,VarChar<1>,long,VarChar<25>,long,VarChar<152>,VarChar<1>,VarChar<25>,long,double>,bool>({});
auto v884 = phmap::flat_hash_map<tuple<VarChar<25>,long>,tuple<double>>({});
auto v886 = phmap::flat_hash_map<tuple<VarChar<25>,long,double>,bool>({});	
	const auto& green = ConstantString("green", 6);

auto v887 = db->na_dataset_size; tbb::parallel_for(tbb::blocked_range<size_t>(0, v887), [&](const tbb::blocked_range<size_t>& v888){auto& v892=v891.local();for (size_t v845=v888.begin(), end=v888.end(); v845!=end; ++v845){v892[n_nationkey[v845]] += phmap::flat_hash_map<tuple<long,VarChar<25>,long,VarChar<152>,VarChar<1>>,bool>({{make_tuple(n_nationkey[v845], n_name[v845], n_regionkey[v845], n_comment[v845], n_NA[v845]), true}});}});for (auto& local : v891)AddMap<phmap::flat_hash_map<long,phmap::flat_hash_map<tuple<long,VarChar<25>,long,VarChar<152>,VarChar<1>>,bool>>,long,phmap::flat_hash_map<tuple<long,VarChar<25>,long,VarChar<152>,VarChar<1>>,bool>>(v846, local);const auto& nation_supplier_build_nest_dict = v846;

auto v894 = db->su_dataset_size; tbb::parallel_for(tbb::blocked_range<size_t>(0, v894), [&](const tbb::blocked_range<size_t>& v895){auto& v899=v898.local();for (size_t v847=v895.begin(), end=v895.end(); v847!=end; ++v847){if (((nation_supplier_build_nest_dict).contains(s_nationkey[v847]))){for (auto& v848 : (nation_supplier_build_nest_dict).at(s_nationkey[v847])){v899[tuple_cat(make_tuple(s_suppkey[v847], s_name[v847], s_address[v847], s_nationkey[v847], s_phone[v847], s_acctbal[v847], s_comment[v847], s_NA[v847]),move(make_tuple(n_nationkey[v848], n_name[v848], n_regionkey[v848], n_comment[v848], n_NA[v848])))] += true;};};}});for (auto& local : v898)AddMap<phmap::flat_hash_map<tuple<long,VarChar<25>,VarChar<40>,long,VarChar<15>,double,VarChar<101>,VarChar<1>,long,VarChar<25>,long,VarChar<152>,VarChar<1>>,bool>,tuple<long,VarChar<25>,VarChar<40>,long,VarChar<15>,double,VarChar<101>,VarChar<1>,long,VarChar<25>,long,VarChar<152>,VarChar<1>>,bool>(v850, local);const auto& nation_supplier_part_partsupp_build_pre_ops = v850;

auto v901 = db->pa_dataset_size; tbb::parallel_for(tbb::blocked_range<size_t>(0, v901), [&](const tbb::blocked_range<size_t>& v902){auto& v906=v905.local();for (size_t v851=v902.begin(), end=v902.end(); v851!=end; ++v851){if (((p_name[v851].firstIndex(green)) != (((long)-1 * (long)1) * (long)1))){v906[make_tuple(p_partkey[v851], p_name[v851], p_mfgr[v851], p_brand[v851], p_type[v851], p_size[v851], p_container[v851], p_retailprice[v851], p_comment[v851], p_NA[v851])] += true;};}});for (auto& local : v905)AddMap<phmap::flat_hash_map<tuple<long,VarChar<55>,VarChar<25>,VarChar<10>,VarChar<25>,long,VarChar<10>,double,VarChar<23>,VarChar<1>>,bool>,tuple<long,VarChar<55>,VarChar<25>,VarChar<10>,VarChar<25>,long,VarChar<10>,double,VarChar<23>,VarChar<1>>,bool>(v852, local);const auto& part_partsupp_build_pre_ops = v852;

for (auto& v853 : part_partsupp_build_pre_ops){v854[ /* p_partkey */get<0>((v853.first))] += phmap::flat_hash_map<tuple<long,VarChar<55>,VarChar<25>,VarChar<10>,VarChar<25>,long,VarChar<10>,double,VarChar<23>,VarChar<1>>,bool>({{(v853.first), (v853.second)}});}const auto& part_partsupp_build_nest_dict = v854;

auto v908 = db->ps_dataset_size; tbb::parallel_for(tbb::blocked_range<size_t>(0, v908), [&](const tbb::blocked_range<size_t>& v909){auto& v913=v912.local();for (size_t v855=v909.begin(), end=v909.end(); v855!=end; ++v855){if (((part_partsupp_build_nest_dict).contains(ps_partkey[v855]))){for (auto& v856 : (part_partsupp_build_nest_dict).at(ps_partkey[v855])){v913[tuple_cat(make_tuple(ps_partkey[v855], ps_suppkey[v855], ps_availqty[v855], ps_supplycost[v855], ps_comment[v855], ps_NA[v855]),move(make_tuple(p_partkey[v856], p_name[v856], p_mfgr[v856], p_brand[v856], p_type[v856], p_size[v856], p_container[v856], p_retailprice[v856], p_comment[v856], p_NA[v856])))] += true;};};}});for (auto& local : v912)AddMap<phmap::flat_hash_map<tuple<long,long,double,double,VarChar<199>,VarChar<1>,long,VarChar<55>,VarChar<25>,VarChar<10>,VarChar<25>,long,VarChar<10>,double,VarChar<23>,VarChar<1>>,bool>,tuple<long,long,double,double,VarChar<199>,VarChar<1>,long,VarChar<55>,VarChar<25>,VarChar<10>,VarChar<25>,long,VarChar<10>,double,VarChar<23>,VarChar<1>>,bool>(v858, local);const auto& nation_supplier_part_partsupp_probe_pre_ops = v858;

for (auto& v859 : nation_supplier_part_partsupp_build_pre_ops){v860[ /* s_suppkey */get<0>((v859.first))] += phmap::flat_hash_map<tuple<long,VarChar<25>,VarChar<40>,long,VarChar<15>,double,VarChar<101>,VarChar<1>,long,VarChar<25>,long,VarChar<152>,VarChar<1>>,bool>({{(v859.first), (v859.second)}});}const auto& nation_supplier_part_partsupp_build_nest_dict = v860;

for (auto& v861 : nation_supplier_part_partsupp_probe_pre_ops){if (((nation_supplier_part_partsupp_build_nest_dict).contains( /* ps_suppkey */get<1>((v861.first))))){for (auto& v862 : (nation_supplier_part_partsupp_build_nest_dict).at( /* ps_suppkey */get<1>((v861.first)))){v864[tuple_cat((v861.first),move((v862.first)))] += true;};};}const auto& nation_supplier_part_partsupp_orders_lineitem_build_pre_ops = v864;

auto v915 = db->ord_dataset_size; tbb::parallel_for(tbb::blocked_range<size_t>(0, v915), [&](const tbb::blocked_range<size_t>& v916){auto& v920=v919.local();for (size_t v865=v916.begin(), end=v916.end(); v865!=end; ++v865){v920[o_orderkey[v865]] += phmap::flat_hash_map<tuple<long,long,VarChar<1>,double,long,VarChar<15>,VarChar<15>,long,VarChar<79>,VarChar<1>>,bool>({{make_tuple(o_orderkey[v865], o_custkey[v865], o_orderstatus[v865], o_totalprice[v865], o_orderdate[v865], o_orderpriority[v865], o_clerk[v865], o_shippriority[v865], o_comment[v865], o_NA[v865]), true}});}});for (auto& local : v919)AddMap<phmap::flat_hash_map<long,phmap::flat_hash_map<tuple<long,long,VarChar<1>,double,long,VarChar<15>,VarChar<15>,long,VarChar<79>,VarChar<1>>,bool>>,long,phmap::flat_hash_map<tuple<long,long,VarChar<1>,double,long,VarChar<15>,VarChar<15>,long,VarChar<79>,VarChar<1>>,bool>>(v866, local);const auto& orders_lineitem_build_nest_dict = v866;

auto v922 = db->li_dataset_size; tbb::parallel_for(tbb::blocked_range<size_t>(0, v922), [&](const tbb::blocked_range<size_t>& v923){auto& v927=v926.local();for (size_t v867=v923.begin(), end=v923.end(); v867!=end; ++v867){if (((orders_lineitem_build_nest_dict).contains(l_orderkey[v867]))){for (auto& v868 : (orders_lineitem_build_nest_dict).at(l_orderkey[v867])){v927[tuple_cat(make_tuple(l_orderkey[v867], l_partkey[v867], l_suppkey[v867], l_linenumber[v867], l_quantity[v867], l_extendedprice[v867], l_discount[v867], l_tax[v867], l_returnflag[v867], l_linestatus[v867], l_shipdate[v867], l_commitdate[v867], l_receiptdate[v867], l_shipinstruct[v867], l_shipmode[v867], l_comment[v867], l_NA[v867]),move(make_tuple(o_orderkey[v868], o_custkey[v868], o_orderstatus[v868], o_totalprice[v868], o_orderdate[v868], o_orderpriority[v868], o_clerk[v868], o_shippriority[v868], o_comment[v868], o_NA[v868])))] += true;};};}});for (auto& local : v926)AddMap<phmap::flat_hash_map<tuple<long,long,long,long,double,double,double,double,VarChar<1>,VarChar<1>,long,long,long,VarChar<25>,VarChar<10>,VarChar<44>,VarChar<1>,long,long,VarChar<1>,double,long,VarChar<15>,VarChar<15>,long,VarChar<79>,VarChar<1>>,bool>,tuple<long,long,long,long,double,double,double,double,VarChar<1>,VarChar<1>,long,long,long,VarChar<25>,VarChar<10>,VarChar<44>,VarChar<1>,long,long,VarChar<1>,double,long,VarChar<15>,VarChar<15>,long,VarChar<79>,VarChar<1>>,bool>(v870, local);const auto& nation_supplier_part_partsupp_orders_lineitem_probe_pre_ops = v870;

for (auto& v871 : nation_supplier_part_partsupp_orders_lineitem_build_pre_ops){v872[make_tuple( /* ps_suppkey */get<1>((v871.first)), /* ps_partkey */get<0>((v871.first)))] += phmap::flat_hash_map<tuple<long,long,double,double,VarChar<199>,VarChar<1>,long,VarChar<55>,VarChar<25>,VarChar<10>,VarChar<25>,long,VarChar<10>,double,VarChar<23>,VarChar<1>,long,VarChar<25>,VarChar<40>,long,VarChar<15>,double,VarChar<101>,VarChar<1>,long,VarChar<25>,long,VarChar<152>,VarChar<1>>,bool>({{(v871.first), (v871.second)}});}const auto& nation_supplier_part_partsupp_orders_lineitem_build_nest_dict = v872;

for (auto& v873 : nation_supplier_part_partsupp_orders_lineitem_probe_pre_ops){if (((nation_supplier_part_partsupp_orders_lineitem_build_nest_dict).contains(make_tuple( /* l_suppkey */get<2>((v873.first)), /* l_partkey */get<1>((v873.first)))))){for (auto& v874 : (nation_supplier_part_partsupp_orders_lineitem_build_nest_dict).at(make_tuple( /* l_suppkey */get<2>((v873.first)), /* l_partkey */get<1>((v873.first))))){v876[tuple_cat((v873.first),move((v874.first)))] += true;};};}const auto& nation_supplier_part_partsupp_orders_lineitem_0 = v876;

for (auto& v877 : nation_supplier_part_partsupp_orders_lineitem_0){v878[tuple_cat((v877.first),move(make_tuple( /* n_name */get<52>((v877.first)))))] += (v877.second);}const auto& nation_supplier_part_partsupp_orders_lineitem_1 = v878;

for (auto& v879 : nation_supplier_part_partsupp_orders_lineitem_1){v880[tuple_cat((v879.first),move(make_tuple((( /* o_orderdate */get<21>((v879.first)))/10000))))] += (v879.second);}const auto& nation_supplier_part_partsupp_orders_lineitem_2 = v880;

for (auto& v881 : nation_supplier_part_partsupp_orders_lineitem_2){v882[tuple_cat((v881.first),move(make_tuple((( /* l_extendedprice */get<5>((v881.first)) * (1.0 -  /* l_discount */get<6>((v881.first)))) - ( /* ps_supplycost */get<30>((v881.first)) *  /* l_quantity */get<4>((v881.first)))))))] += (v881.second);}const auto& nation_supplier_part_partsupp_orders_lineitem_3 = v882;

for (auto& v883 : nation_supplier_part_partsupp_orders_lineitem_3){v884[make_tuple( /* nation */get<56>((v883.first)), /* o_year */get<57>((v883.first)))] += make_tuple( /* amount */get<58>((v883.first)));}const auto& nation_supplier_part_partsupp_orders_lineitem_4 = v884;

for (auto& v885 : nation_supplier_part_partsupp_orders_lineitem_4){v886[tuple_cat((v885.first),move((v885.second)))] += true;}const auto& results = v886;const auto& out = results;

    FastDict_s25if_b* result = (FastDict_s25if_b*)PyObject_CallObject(PyObject_CallObject(PyObject_GetAttrString(PyImport_Import(PyUnicode_FromString("test_unopt_fastdict_compiled")), (char*)"new_FastDict_s25if_b"), nullptr), nullptr);
    *(result->dict) = out;

    return (PyObject*)PyObject_CallObject(PyObject_GetAttrString(PyImport_Import(PyUnicode_FromString("sdqlpy.fastd")), "fastd"), Py_BuildValue("(OO)", result, PyUnicode_FromString("test_unopt")));

}

static PyObject * q10(PyObject *self, PyObject* args)
{
	

    PyObject * db_;
    if (!PyArg_ParseTuple(args, "O", &db_)){
        PyErr_SetString(PyExc_ValueError,"Error while parsing the sum inputs.");
        return NULL;
    }
    
    
    DB dbobj;
    DB* db = &dbobj;
    
const static int numpy_initialized =  init_numpy();


	auto cu_size = PyArray_Size(PyList_GetItem(PyList_GetItem(db_, 0), 0));
	db->cu_dataset_size = cu_size;
	auto c_custkey = (long*)PyArray_DATA(PyList_GetItem(PyList_GetItem(db_, 0), 0));
	auto c_name= (VarChar<25>*)PyArray_DATA(PyList_GetItem(PyList_GetItem(db_, 0), 1));
	auto c_address= (VarChar<40>*)PyArray_DATA(PyList_GetItem(PyList_GetItem(db_, 0), 2));
	auto c_nationkey = (long*)PyArray_DATA(PyList_GetItem(PyList_GetItem(db_, 0), 3));
	auto c_phone= (VarChar<15>*)PyArray_DATA(PyList_GetItem(PyList_GetItem(db_, 0), 4));
	auto c_acctbal = (double*)PyArray_DATA(PyList_GetItem(PyList_GetItem(db_, 0), 5));
	auto c_mktsegment= (VarChar<10>*)PyArray_DATA(PyList_GetItem(PyList_GetItem(db_, 0), 6));
	auto c_comment= (VarChar<117>*)PyArray_DATA(PyList_GetItem(PyList_GetItem(db_, 0), 7));
	auto c_NA= (VarChar<1>*)PyArray_DATA(PyList_GetItem(PyList_GetItem(db_, 0), 8));

	auto ord_size = PyArray_Size(PyList_GetItem(PyList_GetItem(db_, 1), 0));
	db->ord_dataset_size = ord_size;
	auto o_orderkey = (long*)PyArray_DATA(PyList_GetItem(PyList_GetItem(db_, 1), 0));
	auto o_custkey = (long*)PyArray_DATA(PyList_GetItem(PyList_GetItem(db_, 1), 1));
	auto o_orderstatus= (VarChar<1>*)PyArray_DATA(PyList_GetItem(PyList_GetItem(db_, 1), 2));
	auto o_totalprice = (double*)PyArray_DATA(PyList_GetItem(PyList_GetItem(db_, 1), 3));
	auto o_orderdate = (long*)PyArray_DATA(PyList_GetItem(PyList_GetItem(db_, 1), 4));
	auto o_orderpriority= (VarChar<15>*)PyArray_DATA(PyList_GetItem(PyList_GetItem(db_, 1), 5));
	auto o_clerk= (VarChar<15>*)PyArray_DATA(PyList_GetItem(PyList_GetItem(db_, 1), 6));
	auto o_shippriority = (long*)PyArray_DATA(PyList_GetItem(PyList_GetItem(db_, 1), 7));
	auto o_comment= (VarChar<79>*)PyArray_DATA(PyList_GetItem(PyList_GetItem(db_, 1), 8));
	auto o_NA= (VarChar<1>*)PyArray_DATA(PyList_GetItem(PyList_GetItem(db_, 1), 9));

	auto li_size = PyArray_Size(PyList_GetItem(PyList_GetItem(db_, 2), 0));
	db->li_dataset_size = li_size;
	auto l_orderkey = (long*)PyArray_DATA(PyList_GetItem(PyList_GetItem(db_, 2), 0));
	auto l_partkey = (long*)PyArray_DATA(PyList_GetItem(PyList_GetItem(db_, 2), 1));
	auto l_suppkey = (long*)PyArray_DATA(PyList_GetItem(PyList_GetItem(db_, 2), 2));
	auto l_linenumber = (long*)PyArray_DATA(PyList_GetItem(PyList_GetItem(db_, 2), 3));
	auto l_quantity = (double*)PyArray_DATA(PyList_GetItem(PyList_GetItem(db_, 2), 4));
	auto l_extendedprice = (double*)PyArray_DATA(PyList_GetItem(PyList_GetItem(db_, 2), 5));
	auto l_discount = (double*)PyArray_DATA(PyList_GetItem(PyList_GetItem(db_, 2), 6));
	auto l_tax = (double*)PyArray_DATA(PyList_GetItem(PyList_GetItem(db_, 2), 7));
	auto l_returnflag= (VarChar<1>*)PyArray_DATA(PyList_GetItem(PyList_GetItem(db_, 2), 8));
	auto l_linestatus= (VarChar<1>*)PyArray_DATA(PyList_GetItem(PyList_GetItem(db_, 2), 9));
	auto l_shipdate = (long*)PyArray_DATA(PyList_GetItem(PyList_GetItem(db_, 2), 10));
	auto l_commitdate = (long*)PyArray_DATA(PyList_GetItem(PyList_GetItem(db_, 2), 11));
	auto l_receiptdate = (long*)PyArray_DATA(PyList_GetItem(PyList_GetItem(db_, 2), 12));
	auto l_shipinstruct= (VarChar<25>*)PyArray_DATA(PyList_GetItem(PyList_GetItem(db_, 2), 13));
	auto l_shipmode= (VarChar<10>*)PyArray_DATA(PyList_GetItem(PyList_GetItem(db_, 2), 14));
	auto l_comment= (VarChar<44>*)PyArray_DATA(PyList_GetItem(PyList_GetItem(db_, 2), 15));
	auto l_NA= (VarChar<1>*)PyArray_DATA(PyList_GetItem(PyList_GetItem(db_, 2), 16));

	auto na_size = PyArray_Size(PyList_GetItem(PyList_GetItem(db_, 3), 0));
	db->na_dataset_size = na_size;
	auto n_nationkey = (long*)PyArray_DATA(PyList_GetItem(PyList_GetItem(db_, 3), 0));
	auto n_name= (VarChar<25>*)PyArray_DATA(PyList_GetItem(PyList_GetItem(db_, 3), 1));
	auto n_regionkey = (long*)PyArray_DATA(PyList_GetItem(PyList_GetItem(db_, 3), 2));
	auto n_comment= (VarChar<152>*)PyArray_DATA(PyList_GetItem(PyList_GetItem(db_, 3), 3));
	auto n_NA= (VarChar<1>*)PyArray_DATA(PyList_GetItem(PyList_GetItem(db_, 3), 4));



	
auto v958 = phmap::flat_hash_map<tuple<long,long,VarChar<1>,double,long,VarChar<15>,VarChar<15>,long,VarChar<79>,VarChar<1>>,bool>({});
tbb::enumerable_thread_specific<phmap::flat_hash_map<tuple<long,long,VarChar<1>,double,long,VarChar<15>,VarChar<15>,long,VarChar<79>,VarChar<1>>,bool>> v989;
auto v960 = phmap::flat_hash_map<long,phmap::flat_hash_map<tuple<long,VarChar<25>,VarChar<40>,long,VarChar<15>,double,VarChar<10>,VarChar<117>,VarChar<1>>,bool>>({});
tbb::enumerable_thread_specific<phmap::flat_hash_map<long,phmap::flat_hash_map<tuple<long,VarChar<25>,VarChar<40>,long,VarChar<15>,double,VarChar<10>,VarChar<117>,VarChar<1>>,bool>>> v996;
auto v964 = phmap::flat_hash_map<tuple<long,long,VarChar<1>,double,long,VarChar<15>,VarChar<15>,long,VarChar<79>,VarChar<1>,long,VarChar<25>,VarChar<40>,long,VarChar<15>,double,VarChar<10>,VarChar<117>,VarChar<1>>,bool>({});
auto v966 = phmap::flat_hash_map<long,phmap::flat_hash_map<tuple<long,VarChar<25>,long,VarChar<152>,VarChar<1>>,bool>>({});
tbb::enumerable_thread_specific<phmap::flat_hash_map<long,phmap::flat_hash_map<tuple<long,VarChar<25>,long,VarChar<152>,VarChar<1>>,bool>>> v1003;
auto v970 = phmap::flat_hash_map<tuple<long,long,VarChar<1>,double,long,VarChar<15>,VarChar<15>,long,VarChar<79>,VarChar<1>,long,VarChar<25>,VarChar<40>,long,VarChar<15>,double,VarChar<10>,VarChar<117>,VarChar<1>,long,VarChar<25>,long,VarChar<152>,VarChar<1>>,bool>({});
auto v972 = phmap::flat_hash_map<tuple<long,long,long,long,double,double,double,double,VarChar<1>,VarChar<1>,long,long,long,VarChar<25>,VarChar<10>,VarChar<44>,VarChar<1>>,bool>({});
tbb::enumerable_thread_specific<phmap::flat_hash_map<tuple<long,long,long,long,double,double,double,double,VarChar<1>,VarChar<1>,long,long,long,VarChar<25>,VarChar<10>,VarChar<44>,VarChar<1>>,bool>> v1010;
auto v974 = phmap::flat_hash_map<long,phmap::flat_hash_map<tuple<long,long,VarChar<1>,double,long,VarChar<15>,VarChar<15>,long,VarChar<79>,VarChar<1>,long,VarChar<25>,VarChar<40>,long,VarChar<15>,double,VarChar<10>,VarChar<117>,VarChar<1>,long,VarChar<25>,long,VarChar<152>,VarChar<1>>,bool>>({});
auto v978 = phmap::flat_hash_map<tuple<long,long,long,long,double,double,double,double,VarChar<1>,VarChar<1>,long,long,long,VarChar<25>,VarChar<10>,VarChar<44>,VarChar<1>,long,long,VarChar<1>,double,long,VarChar<15>,VarChar<15>,long,VarChar<79>,VarChar<1>,long,VarChar<25>,VarChar<40>,long,VarChar<15>,double,VarChar<10>,VarChar<117>,VarChar<1>,long,VarChar<25>,long,VarChar<152>,VarChar<1>>,bool>({});
auto v980 = phmap::flat_hash_map<tuple<long,long,long,long,double,double,double,double,VarChar<1>,VarChar<1>,long,long,long,VarChar<25>,VarChar<10>,VarChar<44>,VarChar<1>,long,long,VarChar<1>,double,long,VarChar<15>,VarChar<15>,long,VarChar<79>,VarChar<1>,long,VarChar<25>,VarChar<40>,long,VarChar<15>,double,VarChar<10>,VarChar<117>,VarChar<1>,long,VarChar<25>,long,VarChar<152>,VarChar<1>,double>,bool>({});
auto v982 = phmap::flat_hash_map<tuple<long,VarChar<25>,double,VarChar<15>,VarChar<25>,VarChar<40>,VarChar<117>>,tuple<double>>({});
auto v984 = phmap::flat_hash_map<tuple<long,VarChar<25>,double,VarChar<15>,VarChar<25>,VarChar<40>,VarChar<117>,double>,bool>({});	
	const auto& r = ConstantString("R", 2);

auto v985 = db->ord_dataset_size; tbb::parallel_for(tbb::blocked_range<size_t>(0, v985), [&](const tbb::blocked_range<size_t>& v986){auto& v990=v989.local();for (size_t v957=v986.begin(), end=v986.end(); v957!=end; ++v957){if (((o_orderdate[v957] >= (long)19931001) && (o_orderdate[v957] < (long)19940101))){v990[make_tuple(o_orderkey[v957], o_custkey[v957], o_orderstatus[v957], o_totalprice[v957], o_orderdate[v957], o_orderpriority[v957], o_clerk[v957], o_shippriority[v957], o_comment[v957], o_NA[v957])] += true;};}});for (auto& local : v989)AddMap<phmap::flat_hash_map<tuple<long,long,VarChar<1>,double,long,VarChar<15>,VarChar<15>,long,VarChar<79>,VarChar<1>>,bool>,tuple<long,long,VarChar<1>,double,long,VarChar<15>,VarChar<15>,long,VarChar<79>,VarChar<1>>,bool>(v958, local);const auto& customer_orders_probe_pre_ops = v958;

auto v992 = db->cu_dataset_size; tbb::parallel_for(tbb::blocked_range<size_t>(0, v992), [&](const tbb::blocked_range<size_t>& v993){auto& v997=v996.local();for (size_t v959=v993.begin(), end=v993.end(); v959!=end; ++v959){v997[c_custkey[v959]] += phmap::flat_hash_map<tuple<long,VarChar<25>,VarChar<40>,long,VarChar<15>,double,VarChar<10>,VarChar<117>,VarChar<1>>,bool>({{make_tuple(c_custkey[v959], c_name[v959], c_address[v959], c_nationkey[v959], c_phone[v959], c_acctbal[v959], c_mktsegment[v959], c_comment[v959], c_NA[v959]), true}});}});for (auto& local : v996)AddMap<phmap::flat_hash_map<long,phmap::flat_hash_map<tuple<long,VarChar<25>,VarChar<40>,long,VarChar<15>,double,VarChar<10>,VarChar<117>,VarChar<1>>,bool>>,long,phmap::flat_hash_map<tuple<long,VarChar<25>,VarChar<40>,long,VarChar<15>,double,VarChar<10>,VarChar<117>,VarChar<1>>,bool>>(v960, local);const auto& customer_orders_build_nest_dict = v960;

for (auto& v961 : customer_orders_probe_pre_ops){if (((customer_orders_build_nest_dict).contains( /* o_custkey */get<1>((v961.first))))){for (auto& v962 : (customer_orders_build_nest_dict).at( /* o_custkey */get<1>((v961.first)))){v964[tuple_cat((v961.first),move((v962.first)))] += true;};};}const auto& nation_customer_orders_probe_pre_ops = v964;

auto v999 = db->na_dataset_size; tbb::parallel_for(tbb::blocked_range<size_t>(0, v999), [&](const tbb::blocked_range<size_t>& v1000){auto& v1004=v1003.local();for (size_t v965=v1000.begin(), end=v1000.end(); v965!=end; ++v965){v1004[n_nationkey[v965]] += phmap::flat_hash_map<tuple<long,VarChar<25>,long,VarChar<152>,VarChar<1>>,bool>({{make_tuple(n_nationkey[v965], n_name[v965], n_regionkey[v965], n_comment[v965], n_NA[v965]), true}});}});for (auto& local : v1003)AddMap<phmap::flat_hash_map<long,phmap::flat_hash_map<tuple<long,VarChar<25>,long,VarChar<152>,VarChar<1>>,bool>>,long,phmap::flat_hash_map<tuple<long,VarChar<25>,long,VarChar<152>,VarChar<1>>,bool>>(v966, local);const auto& nation_customer_orders_build_nest_dict = v966;

for (auto& v967 : nation_customer_orders_probe_pre_ops){if (((nation_customer_orders_build_nest_dict).contains( /* c_nationkey */get<13>((v967.first))))){for (auto& v968 : (nation_customer_orders_build_nest_dict).at( /* c_nationkey */get<13>((v967.first)))){v970[tuple_cat((v967.first),move((v968.first)))] += true;};};}const auto& nation_customer_orders_lineitem_build_pre_ops = v970;

auto v1006 = db->li_dataset_size; tbb::parallel_for(tbb::blocked_range<size_t>(0, v1006), [&](const tbb::blocked_range<size_t>& v1007){auto& v1011=v1010.local();for (size_t v971=v1007.begin(), end=v1007.end(); v971!=end; ++v971){if ((l_returnflag[v971] == r)){v1011[make_tuple(l_orderkey[v971], l_partkey[v971], l_suppkey[v971], l_linenumber[v971], l_quantity[v971], l_extendedprice[v971], l_discount[v971], l_tax[v971], l_returnflag[v971], l_linestatus[v971], l_shipdate[v971], l_commitdate[v971], l_receiptdate[v971], l_shipinstruct[v971], l_shipmode[v971], l_comment[v971], l_NA[v971])] += true;};}});for (auto& local : v1010)AddMap<phmap::flat_hash_map<tuple<long,long,long,long,double,double,double,double,VarChar<1>,VarChar<1>,long,long,long,VarChar<25>,VarChar<10>,VarChar<44>,VarChar<1>>,bool>,tuple<long,long,long,long,double,double,double,double,VarChar<1>,VarChar<1>,long,long,long,VarChar<25>,VarChar<10>,VarChar<44>,VarChar<1>>,bool>(v972, local);const auto& nation_customer_orders_lineitem_probe_pre_ops = v972;

for (auto& v973 : nation_customer_orders_lineitem_build_pre_ops){v974[ /* o_orderkey */get<0>((v973.first))] += phmap::flat_hash_map<tuple<long,long,VarChar<1>,double,long,VarChar<15>,VarChar<15>,long,VarChar<79>,VarChar<1>,long,VarChar<25>,VarChar<40>,long,VarChar<15>,double,VarChar<10>,VarChar<117>,VarChar<1>,long,VarChar<25>,long,VarChar<152>,VarChar<1>>,bool>({{(v973.first), (v973.second)}});}const auto& nation_customer_orders_lineitem_build_nest_dict = v974;

for (auto& v975 : nation_customer_orders_lineitem_probe_pre_ops){if (((nation_customer_orders_lineitem_build_nest_dict).contains( /* l_orderkey */get<0>((v975.first))))){for (auto& v976 : (nation_customer_orders_lineitem_build_nest_dict).at( /* l_orderkey */get<0>((v975.first)))){v978[tuple_cat((v975.first),move((v976.first)))] += true;};};}const auto& nation_customer_orders_lineitem_0 = v978;

for (auto& v979 : nation_customer_orders_lineitem_0){v980[tuple_cat((v979.first),move(make_tuple(( /* l_extendedprice */get<5>((v979.first)) * (1.0 -  /* l_discount */get<6>((v979.first)))))))] += (v979.second);}const auto& nation_customer_orders_lineitem_1 = v980;

for (auto& v981 : nation_customer_orders_lineitem_1){v982[make_tuple( /* c_custkey */get<27>((v981.first)), /* c_name */get<28>((v981.first)), /* c_acctbal */get<32>((v981.first)), /* c_phone */get<31>((v981.first)), /* n_name */get<37>((v981.first)), /* c_address */get<29>((v981.first)), /* c_comment */get<34>((v981.first)))] += make_tuple( /* revenue */get<41>((v981.first)));}const auto& nation_customer_orders_lineitem_2 = v982;

for (auto& v983 : nation_customer_orders_lineitem_2){v984[tuple_cat((v983.first),move((v983.second)))] += true;}const auto& results = v984;const auto& out = results;

    FastDict_is25fs15s25s40s117f_b* result = (FastDict_is25fs15s25s40s117f_b*)PyObject_CallObject(PyObject_CallObject(PyObject_GetAttrString(PyImport_Import(PyUnicode_FromString("test_unopt_fastdict_compiled")), (char*)"new_FastDict_is25fs15s25s40s117f_b"), nullptr), nullptr);
    *(result->dict) = out;

    return (PyObject*)PyObject_CallObject(PyObject_GetAttrString(PyImport_Import(PyUnicode_FromString("sdqlpy.fastd")), "fastd"), Py_BuildValue("(OO)", result, PyUnicode_FromString("test_unopt")));

}

static PyObject * q11(PyObject *self, PyObject* args)
{
	

    PyObject * db_;
    if (!PyArg_ParseTuple(args, "O", &db_)){
        PyErr_SetString(PyExc_ValueError,"Error while parsing the sum inputs.");
        return NULL;
    }
    
    
    DB dbobj;
    DB* db = &dbobj;
    
const static int numpy_initialized =  init_numpy();


	auto ps_size = PyArray_Size(PyList_GetItem(PyList_GetItem(db_, 0), 0));
	db->ps_dataset_size = ps_size;
	auto ps_partkey = (long*)PyArray_DATA(PyList_GetItem(PyList_GetItem(db_, 0), 0));
	auto ps_suppkey = (long*)PyArray_DATA(PyList_GetItem(PyList_GetItem(db_, 0), 1));
	auto ps_availqty = (double*)PyArray_DATA(PyList_GetItem(PyList_GetItem(db_, 0), 2));
	auto ps_supplycost = (double*)PyArray_DATA(PyList_GetItem(PyList_GetItem(db_, 0), 3));
	auto ps_comment= (VarChar<199>*)PyArray_DATA(PyList_GetItem(PyList_GetItem(db_, 0), 4));
	auto ps_NA= (VarChar<1>*)PyArray_DATA(PyList_GetItem(PyList_GetItem(db_, 0), 5));

	auto su_size = PyArray_Size(PyList_GetItem(PyList_GetItem(db_, 1), 0));
	db->su_dataset_size = su_size;
	auto s_suppkey = (long*)PyArray_DATA(PyList_GetItem(PyList_GetItem(db_, 1), 0));
	auto s_name= (VarChar<25>*)PyArray_DATA(PyList_GetItem(PyList_GetItem(db_, 1), 1));
	auto s_address= (VarChar<40>*)PyArray_DATA(PyList_GetItem(PyList_GetItem(db_, 1), 2));
	auto s_nationkey = (long*)PyArray_DATA(PyList_GetItem(PyList_GetItem(db_, 1), 3));
	auto s_phone= (VarChar<15>*)PyArray_DATA(PyList_GetItem(PyList_GetItem(db_, 1), 4));
	auto s_acctbal = (double*)PyArray_DATA(PyList_GetItem(PyList_GetItem(db_, 1), 5));
	auto s_comment= (VarChar<101>*)PyArray_DATA(PyList_GetItem(PyList_GetItem(db_, 1), 6));
	auto s_NA= (VarChar<1>*)PyArray_DATA(PyList_GetItem(PyList_GetItem(db_, 1), 7));

	auto na_size = PyArray_Size(PyList_GetItem(PyList_GetItem(db_, 2), 0));
	db->na_dataset_size = na_size;
	auto n_nationkey = (long*)PyArray_DATA(PyList_GetItem(PyList_GetItem(db_, 2), 0));
	auto n_name= (VarChar<25>*)PyArray_DATA(PyList_GetItem(PyList_GetItem(db_, 2), 1));
	auto n_regionkey = (long*)PyArray_DATA(PyList_GetItem(PyList_GetItem(db_, 2), 2));
	auto n_comment= (VarChar<152>*)PyArray_DATA(PyList_GetItem(PyList_GetItem(db_, 2), 3));
	auto n_NA= (VarChar<1>*)PyArray_DATA(PyList_GetItem(PyList_GetItem(db_, 2), 4));



	
auto v1042 = phmap::flat_hash_map<tuple<long,VarChar<25>,long,VarChar<152>,VarChar<1>>,bool>({});
tbb::enumerable_thread_specific<phmap::flat_hash_map<tuple<long,VarChar<25>,long,VarChar<152>,VarChar<1>>,bool>> v1073;
auto v1044 = phmap::flat_hash_map<long,phmap::flat_hash_map<tuple<long,VarChar<25>,long,VarChar<152>,VarChar<1>>,bool>>({});
auto v1048 = phmap::flat_hash_map<tuple<long,VarChar<25>,VarChar<40>,long,VarChar<15>,double,VarChar<101>,VarChar<1>,long,VarChar<25>,long,VarChar<152>,VarChar<1>>,bool>({});
tbb::enumerable_thread_specific<phmap::flat_hash_map<tuple<long,VarChar<25>,VarChar<40>,long,VarChar<15>,double,VarChar<101>,VarChar<1>,long,VarChar<25>,long,VarChar<152>,VarChar<1>>,bool>> v1080;
auto v1050 = phmap::flat_hash_map<long,phmap::flat_hash_map<tuple<long,VarChar<25>,VarChar<40>,long,VarChar<15>,double,VarChar<101>,VarChar<1>,long,VarChar<25>,long,VarChar<152>,VarChar<1>>,bool>>({});
auto v1054 = phmap::flat_hash_map<tuple<long,long,double,double,VarChar<199>,VarChar<1>,long,VarChar<25>,VarChar<40>,long,VarChar<15>,double,VarChar<101>,VarChar<1>,long,VarChar<25>,long,VarChar<152>,VarChar<1>>,bool>({});
tbb::enumerable_thread_specific<phmap::flat_hash_map<tuple<long,long,double,double,VarChar<199>,VarChar<1>,long,VarChar<25>,VarChar<40>,long,VarChar<15>,double,VarChar<101>,VarChar<1>,long,VarChar<25>,long,VarChar<152>,VarChar<1>>,bool>> v1087;
auto v1056 = 0.0;
auto v1058 = phmap::flat_hash_map<tuple<long>,double>({});
auto v1060 = phmap::flat_hash_map<tuple<long>,bool>({});
auto v1062 = phmap::flat_hash_map<tuple<long,long,double,double,VarChar<199>,VarChar<1>,long,VarChar<25>,VarChar<40>,long,VarChar<15>,double,VarChar<101>,VarChar<1>,long,VarChar<25>,long,VarChar<152>,VarChar<1>>,bool>({});
auto v1064 = phmap::flat_hash_map<tuple<long,long,double,double,VarChar<199>,VarChar<1>,long,VarChar<25>,VarChar<40>,long,VarChar<15>,double,VarChar<101>,VarChar<1>,long,VarChar<25>,long,VarChar<152>,VarChar<1>,double>,bool>({});
auto v1066 = phmap::flat_hash_map<tuple<long>,tuple<double>>({});
auto v1068 = phmap::flat_hash_map<tuple<long,double>,bool>({});	
	const auto& germany = ConstantString("GERMANY", 8);

auto v1069 = db->na_dataset_size; tbb::parallel_for(tbb::blocked_range<size_t>(0, v1069), [&](const tbb::blocked_range<size_t>& v1070){auto& v1074=v1073.local();for (size_t v1041=v1070.begin(), end=v1070.end(); v1041!=end; ++v1041){if ((n_name[v1041] == germany)){v1074[make_tuple(n_nationkey[v1041], n_name[v1041], n_regionkey[v1041], n_comment[v1041], n_NA[v1041])] += true;};}});for (auto& local : v1073)AddMap<phmap::flat_hash_map<tuple<long,VarChar<25>,long,VarChar<152>,VarChar<1>>,bool>,tuple<long,VarChar<25>,long,VarChar<152>,VarChar<1>>,bool>(v1042, local);const auto& nation_supplier_build_pre_ops = v1042;

for (auto& v1043 : nation_supplier_build_pre_ops){v1044[ /* n_nationkey */get<0>((v1043.first))] += phmap::flat_hash_map<tuple<long,VarChar<25>,long,VarChar<152>,VarChar<1>>,bool>({{(v1043.first), (v1043.second)}});}const auto& nation_supplier_build_nest_dict = v1044;

auto v1076 = db->su_dataset_size; tbb::parallel_for(tbb::blocked_range<size_t>(0, v1076), [&](const tbb::blocked_range<size_t>& v1077){auto& v1081=v1080.local();for (size_t v1045=v1077.begin(), end=v1077.end(); v1045!=end; ++v1045){if (((nation_supplier_build_nest_dict).contains(s_nationkey[v1045]))){for (auto& v1046 : (nation_supplier_build_nest_dict).at(s_nationkey[v1045])){v1081[tuple_cat(make_tuple(s_suppkey[v1045], s_name[v1045], s_address[v1045], s_nationkey[v1045], s_phone[v1045], s_acctbal[v1045], s_comment[v1045], s_NA[v1045]),move(make_tuple(n_nationkey[v1046], n_name[v1046], n_regionkey[v1046], n_comment[v1046], n_NA[v1046])))] += true;};};}});for (auto& local : v1080)AddMap<phmap::flat_hash_map<tuple<long,VarChar<25>,VarChar<40>,long,VarChar<15>,double,VarChar<101>,VarChar<1>,long,VarChar<25>,long,VarChar<152>,VarChar<1>>,bool>,tuple<long,VarChar<25>,VarChar<40>,long,VarChar<15>,double,VarChar<101>,VarChar<1>,long,VarChar<25>,long,VarChar<152>,VarChar<1>>,bool>(v1048, local);const auto& nation_supplier_partsupp_build_pre_ops = v1048;

for (auto& v1049 : nation_supplier_partsupp_build_pre_ops){v1050[ /* s_suppkey */get<0>((v1049.first))] += phmap::flat_hash_map<tuple<long,VarChar<25>,VarChar<40>,long,VarChar<15>,double,VarChar<101>,VarChar<1>,long,VarChar<25>,long,VarChar<152>,VarChar<1>>,bool>({{(v1049.first), (v1049.second)}});}const auto& nation_supplier_partsupp_build_nest_dict = v1050;

auto v1083 = db->ps_dataset_size; tbb::parallel_for(tbb::blocked_range<size_t>(0, v1083), [&](const tbb::blocked_range<size_t>& v1084){auto& v1088=v1087.local();for (size_t v1051=v1084.begin(), end=v1084.end(); v1051!=end; ++v1051){if (((nation_supplier_partsupp_build_nest_dict).contains(ps_suppkey[v1051]))){for (auto& v1052 : (nation_supplier_partsupp_build_nest_dict).at(ps_suppkey[v1051])){v1088[tuple_cat(make_tuple(ps_partkey[v1051], ps_suppkey[v1051], ps_availqty[v1051], ps_supplycost[v1051], ps_comment[v1051], ps_NA[v1051]),move(make_tuple(s_suppkey[v1052], s_name[v1052], s_address[v1052], s_nationkey[v1052], s_phone[v1052], s_acctbal[v1052], s_comment[v1052], s_NA[v1052], n_nationkey[v1052], n_name[v1052], n_regionkey[v1052], n_comment[v1052], n_NA[v1052])))] += true;};};}});for (auto& local : v1087)AddMap<phmap::flat_hash_map<tuple<long,long,double,double,VarChar<199>,VarChar<1>,long,VarChar<25>,VarChar<40>,long,VarChar<15>,double,VarChar<101>,VarChar<1>,long,VarChar<25>,long,VarChar<152>,VarChar<1>>,bool>,tuple<long,long,double,double,VarChar<199>,VarChar<1>,long,VarChar<25>,VarChar<40>,long,VarChar<15>,double,VarChar<101>,VarChar<1>,long,VarChar<25>,long,VarChar<152>,VarChar<1>>,bool>(v1054, local);const auto& nation_supplier_partsupp_0 = v1054;

for (auto& v1055 : nation_supplier_partsupp_0){v1056 += (( /* ps_supplycost */get<3>((v1055.first)) *  /* ps_availqty */get<2>((v1055.first))) * 0.0001);}const auto& tmp_var_JQ_JQ_ps_supplycost_mul_ps_availqty_XZ_mul_00001_XZ = v1056;

for (auto& v1057 : nation_supplier_partsupp_0){v1058[make_tuple( /* ps_partkey */get<0>((v1057.first)))] += ( /* ps_supplycost */get<3>((v1057.first)) *  /* ps_availqty */get<2>((v1057.first)));}const auto& nation_supplier_partsupp_1 = v1058;

for (auto& v1059 : nation_supplier_partsupp_1){if ((tmp_var_JQ_JQ_ps_supplycost_mul_ps_availqty_XZ_mul_00001_XZ < (v1059.second))){v1060[(v1059.first)] += true;};}const auto& nation_supplier_partsupp_2 = v1060;

for (auto& v1061 : nation_supplier_partsupp_0){if (((nation_supplier_partsupp_2).contains(make_tuple( /* ps_partkey */get<0>((v1061.first)))))){v1062[(v1061.first)] += true;};}const auto& nation_supplier_partsupp_3 = v1062;

for (auto& v1063 : nation_supplier_partsupp_3){v1064[tuple_cat((v1063.first),move(make_tuple(( /* ps_supplycost */get<3>((v1063.first)) *  /* ps_availqty */get<2>((v1063.first))))))] += (v1063.second);}const auto& nation_supplier_partsupp_4 = v1064;

for (auto& v1065 : nation_supplier_partsupp_4){v1066[make_tuple( /* ps_partkey */get<0>((v1065.first)))] += make_tuple( /* value */get<19>((v1065.first)));}const auto& nation_supplier_partsupp_5 = v1066;

for (auto& v1067 : nation_supplier_partsupp_5){v1068[tuple_cat((v1067.first),move((v1067.second)))] += true;}const auto& results = v1068;const auto& out = results;

    FastDict_if_b* result = (FastDict_if_b*)PyObject_CallObject(PyObject_CallObject(PyObject_GetAttrString(PyImport_Import(PyUnicode_FromString("test_unopt_fastdict_compiled")), (char*)"new_FastDict_if_b"), nullptr), nullptr);
    *(result->dict) = out;

    return (PyObject*)PyObject_CallObject(PyObject_GetAttrString(PyImport_Import(PyUnicode_FromString("sdqlpy.fastd")), "fastd"), Py_BuildValue("(OO)", result, PyUnicode_FromString("test_unopt")));

}

static PyObject * q12(PyObject *self, PyObject* args)
{
	

    PyObject * db_;
    if (!PyArg_ParseTuple(args, "O", &db_)){
        PyErr_SetString(PyExc_ValueError,"Error while parsing the sum inputs.");
        return NULL;
    }
    
    
    DB dbobj;
    DB* db = &dbobj;
    
const static int numpy_initialized =  init_numpy();


	auto ord_size = PyArray_Size(PyList_GetItem(PyList_GetItem(db_, 0), 0));
	db->ord_dataset_size = ord_size;
	auto o_orderkey = (long*)PyArray_DATA(PyList_GetItem(PyList_GetItem(db_, 0), 0));
	auto o_custkey = (long*)PyArray_DATA(PyList_GetItem(PyList_GetItem(db_, 0), 1));
	auto o_orderstatus= (VarChar<1>*)PyArray_DATA(PyList_GetItem(PyList_GetItem(db_, 0), 2));
	auto o_totalprice = (double*)PyArray_DATA(PyList_GetItem(PyList_GetItem(db_, 0), 3));
	auto o_orderdate = (long*)PyArray_DATA(PyList_GetItem(PyList_GetItem(db_, 0), 4));
	auto o_orderpriority= (VarChar<15>*)PyArray_DATA(PyList_GetItem(PyList_GetItem(db_, 0), 5));
	auto o_clerk= (VarChar<15>*)PyArray_DATA(PyList_GetItem(PyList_GetItem(db_, 0), 6));
	auto o_shippriority = (long*)PyArray_DATA(PyList_GetItem(PyList_GetItem(db_, 0), 7));
	auto o_comment= (VarChar<79>*)PyArray_DATA(PyList_GetItem(PyList_GetItem(db_, 0), 8));
	auto o_NA= (VarChar<1>*)PyArray_DATA(PyList_GetItem(PyList_GetItem(db_, 0), 9));

	auto li_size = PyArray_Size(PyList_GetItem(PyList_GetItem(db_, 1), 0));
	db->li_dataset_size = li_size;
	auto l_orderkey = (long*)PyArray_DATA(PyList_GetItem(PyList_GetItem(db_, 1), 0));
	auto l_partkey = (long*)PyArray_DATA(PyList_GetItem(PyList_GetItem(db_, 1), 1));
	auto l_suppkey = (long*)PyArray_DATA(PyList_GetItem(PyList_GetItem(db_, 1), 2));
	auto l_linenumber = (long*)PyArray_DATA(PyList_GetItem(PyList_GetItem(db_, 1), 3));
	auto l_quantity = (double*)PyArray_DATA(PyList_GetItem(PyList_GetItem(db_, 1), 4));
	auto l_extendedprice = (double*)PyArray_DATA(PyList_GetItem(PyList_GetItem(db_, 1), 5));
	auto l_discount = (double*)PyArray_DATA(PyList_GetItem(PyList_GetItem(db_, 1), 6));
	auto l_tax = (double*)PyArray_DATA(PyList_GetItem(PyList_GetItem(db_, 1), 7));
	auto l_returnflag= (VarChar<1>*)PyArray_DATA(PyList_GetItem(PyList_GetItem(db_, 1), 8));
	auto l_linestatus= (VarChar<1>*)PyArray_DATA(PyList_GetItem(PyList_GetItem(db_, 1), 9));
	auto l_shipdate = (long*)PyArray_DATA(PyList_GetItem(PyList_GetItem(db_, 1), 10));
	auto l_commitdate = (long*)PyArray_DATA(PyList_GetItem(PyList_GetItem(db_, 1), 11));
	auto l_receiptdate = (long*)PyArray_DATA(PyList_GetItem(PyList_GetItem(db_, 1), 12));
	auto l_shipinstruct= (VarChar<25>*)PyArray_DATA(PyList_GetItem(PyList_GetItem(db_, 1), 13));
	auto l_shipmode= (VarChar<10>*)PyArray_DATA(PyList_GetItem(PyList_GetItem(db_, 1), 14));
	auto l_comment= (VarChar<44>*)PyArray_DATA(PyList_GetItem(PyList_GetItem(db_, 1), 15));
	auto l_NA= (VarChar<1>*)PyArray_DATA(PyList_GetItem(PyList_GetItem(db_, 1), 16));



	
auto v1107 = phmap::flat_hash_map<tuple<long,long,long,long,double,double,double,double,VarChar<1>,VarChar<1>,long,long,long,VarChar<25>,VarChar<10>,VarChar<44>,VarChar<1>>,bool>({});
tbb::enumerable_thread_specific<phmap::flat_hash_map<tuple<long,long,long,long,double,double,double,double,VarChar<1>,VarChar<1>,long,long,long,VarChar<25>,VarChar<10>,VarChar<44>,VarChar<1>>,bool>> v1126;
auto v1109 = phmap::flat_hash_map<long,phmap::flat_hash_map<tuple<long,long,VarChar<1>,double,long,VarChar<15>,VarChar<15>,long,VarChar<79>,VarChar<1>>,bool>>({});
tbb::enumerable_thread_specific<phmap::flat_hash_map<long,phmap::flat_hash_map<tuple<long,long,VarChar<1>,double,long,VarChar<15>,VarChar<15>,long,VarChar<79>,VarChar<1>>,bool>>> v1133;
auto v1113 = phmap::flat_hash_map<tuple<long,long,long,long,double,double,double,double,VarChar<1>,VarChar<1>,long,long,long,VarChar<25>,VarChar<10>,VarChar<44>,VarChar<1>,long,long,VarChar<1>,double,long,VarChar<15>,VarChar<15>,long,VarChar<79>,VarChar<1>>,bool>({});
auto v1115 = phmap::flat_hash_map<tuple<long,long,long,long,double,double,double,double,VarChar<1>,VarChar<1>,long,long,long,VarChar<25>,VarChar<10>,VarChar<44>,VarChar<1>,long,long,VarChar<1>,double,long,VarChar<15>,VarChar<15>,long,VarChar<79>,VarChar<1>,long>,bool>({});
auto v1117 = phmap::flat_hash_map<tuple<long,long,long,long,double,double,double,double,VarChar<1>,VarChar<1>,long,long,long,VarChar<25>,VarChar<10>,VarChar<44>,VarChar<1>,long,long,VarChar<1>,double,long,VarChar<15>,VarChar<15>,long,VarChar<79>,VarChar<1>,long,long>,bool>({});
auto v1119 = phmap::flat_hash_map<tuple<VarChar<10>>,tuple<long,long>>({});
auto v1121 = phmap::flat_hash_map<tuple<VarChar<10>,long,long>,bool>({});	
	const auto& mail = ConstantString("MAIL", 5);const auto& ship = ConstantString("SHIP", 5);const auto& urgent1 = ConstantString("1-URGENT", 9);const auto& high2 = ConstantString("2-HIGH", 7);

auto v1122 = db->li_dataset_size; tbb::parallel_for(tbb::blocked_range<size_t>(0, v1122), [&](const tbb::blocked_range<size_t>& v1123){auto& v1127=v1126.local();for (size_t v1106=v1123.begin(), end=v1123.end(); v1106!=end; ++v1106){if (((((((l_shipmode[v1106] == ship) || (l_shipmode[v1106] == mail)) && (l_commitdate[v1106] < l_receiptdate[v1106])) && (l_shipdate[v1106] < l_commitdate[v1106])) && (l_receiptdate[v1106] >= (long)19940101)) && (l_receiptdate[v1106] < (long)19950101))){v1127[make_tuple(l_orderkey[v1106], l_partkey[v1106], l_suppkey[v1106], l_linenumber[v1106], l_quantity[v1106], l_extendedprice[v1106], l_discount[v1106], l_tax[v1106], l_returnflag[v1106], l_linestatus[v1106], l_shipdate[v1106], l_commitdate[v1106], l_receiptdate[v1106], l_shipinstruct[v1106], l_shipmode[v1106], l_comment[v1106], l_NA[v1106])] += true;};}});for (auto& local : v1126)AddMap<phmap::flat_hash_map<tuple<long,long,long,long,double,double,double,double,VarChar<1>,VarChar<1>,long,long,long,VarChar<25>,VarChar<10>,VarChar<44>,VarChar<1>>,bool>,tuple<long,long,long,long,double,double,double,double,VarChar<1>,VarChar<1>,long,long,long,VarChar<25>,VarChar<10>,VarChar<44>,VarChar<1>>,bool>(v1107, local);const auto& orders_lineitem_probe_pre_ops = v1107;

auto v1129 = db->ord_dataset_size; tbb::parallel_for(tbb::blocked_range<size_t>(0, v1129), [&](const tbb::blocked_range<size_t>& v1130){auto& v1134=v1133.local();for (size_t v1108=v1130.begin(), end=v1130.end(); v1108!=end; ++v1108){v1134[o_orderkey[v1108]] += phmap::flat_hash_map<tuple<long,long,VarChar<1>,double,long,VarChar<15>,VarChar<15>,long,VarChar<79>,VarChar<1>>,bool>({{make_tuple(o_orderkey[v1108], o_custkey[v1108], o_orderstatus[v1108], o_totalprice[v1108], o_orderdate[v1108], o_orderpriority[v1108], o_clerk[v1108], o_shippriority[v1108], o_comment[v1108], o_NA[v1108]), true}});}});for (auto& local : v1133)AddMap<phmap::flat_hash_map<long,phmap::flat_hash_map<tuple<long,long,VarChar<1>,double,long,VarChar<15>,VarChar<15>,long,VarChar<79>,VarChar<1>>,bool>>,long,phmap::flat_hash_map<tuple<long,long,VarChar<1>,double,long,VarChar<15>,VarChar<15>,long,VarChar<79>,VarChar<1>>,bool>>(v1109, local);const auto& orders_lineitem_build_nest_dict = v1109;

for (auto& v1110 : orders_lineitem_probe_pre_ops){if (((orders_lineitem_build_nest_dict).contains( /* l_orderkey */get<0>((v1110.first))))){for (auto& v1111 : (orders_lineitem_build_nest_dict).at( /* l_orderkey */get<0>((v1110.first)))){v1113[tuple_cat((v1110.first),move((v1111.first)))] += true;};};}const auto& orders_lineitem_0 = v1113;

for (auto& v1114 : orders_lineitem_0){v1115[tuple_cat((v1114.first),move(make_tuple(((( /* o_orderpriority */get<22>((v1114.first)) == urgent1) || ( /* o_orderpriority */get<22>((v1114.first)) == high2))) ? ((long)1) : ((long)0))))] += (v1114.second);}const auto& orders_lineitem_1 = v1115;

for (auto& v1116 : orders_lineitem_1){v1117[tuple_cat((v1116.first),move(make_tuple(((( /* o_orderpriority */get<22>((v1116.first)) != urgent1) && ( /* o_orderpriority */get<22>((v1116.first)) != high2))) ? ((long)1) : ((long)0))))] += (v1116.second);}const auto& orders_lineitem_2 = v1117;

for (auto& v1118 : orders_lineitem_2){v1119[make_tuple( /* l_shipmode */get<14>((v1118.first)))] += make_tuple( /* high_line_priority */get<27>((v1118.first)), /* low_line_priority */get<28>((v1118.first)));}const auto& orders_lineitem_3 = v1119;

for (auto& v1120 : orders_lineitem_3){v1121[tuple_cat((v1120.first),move((v1120.second)))] += true;}const auto& results = v1121;const auto& out = results;

    FastDict_s10ii_b* result = (FastDict_s10ii_b*)PyObject_CallObject(PyObject_CallObject(PyObject_GetAttrString(PyImport_Import(PyUnicode_FromString("test_unopt_fastdict_compiled")), (char*)"new_FastDict_s10ii_b"), nullptr), nullptr);
    *(result->dict) = out;

    return (PyObject*)PyObject_CallObject(PyObject_GetAttrString(PyImport_Import(PyUnicode_FromString("sdqlpy.fastd")), "fastd"), Py_BuildValue("(OO)", result, PyUnicode_FromString("test_unopt")));

}

static PyObject * q14(PyObject *self, PyObject* args)
{
	

    PyObject * db_;
    if (!PyArg_ParseTuple(args, "O", &db_)){
        PyErr_SetString(PyExc_ValueError,"Error while parsing the sum inputs.");
        return NULL;
    }
    
    
    DB dbobj;
    DB* db = &dbobj;
    
const static int numpy_initialized =  init_numpy();


	auto li_size = PyArray_Size(PyList_GetItem(PyList_GetItem(db_, 0), 0));
	db->li_dataset_size = li_size;
	auto l_orderkey = (long*)PyArray_DATA(PyList_GetItem(PyList_GetItem(db_, 0), 0));
	auto l_partkey = (long*)PyArray_DATA(PyList_GetItem(PyList_GetItem(db_, 0), 1));
	auto l_suppkey = (long*)PyArray_DATA(PyList_GetItem(PyList_GetItem(db_, 0), 2));
	auto l_linenumber = (long*)PyArray_DATA(PyList_GetItem(PyList_GetItem(db_, 0), 3));
	auto l_quantity = (double*)PyArray_DATA(PyList_GetItem(PyList_GetItem(db_, 0), 4));
	auto l_extendedprice = (double*)PyArray_DATA(PyList_GetItem(PyList_GetItem(db_, 0), 5));
	auto l_discount = (double*)PyArray_DATA(PyList_GetItem(PyList_GetItem(db_, 0), 6));
	auto l_tax = (double*)PyArray_DATA(PyList_GetItem(PyList_GetItem(db_, 0), 7));
	auto l_returnflag= (VarChar<1>*)PyArray_DATA(PyList_GetItem(PyList_GetItem(db_, 0), 8));
	auto l_linestatus= (VarChar<1>*)PyArray_DATA(PyList_GetItem(PyList_GetItem(db_, 0), 9));
	auto l_shipdate = (long*)PyArray_DATA(PyList_GetItem(PyList_GetItem(db_, 0), 10));
	auto l_commitdate = (long*)PyArray_DATA(PyList_GetItem(PyList_GetItem(db_, 0), 11));
	auto l_receiptdate = (long*)PyArray_DATA(PyList_GetItem(PyList_GetItem(db_, 0), 12));
	auto l_shipinstruct= (VarChar<25>*)PyArray_DATA(PyList_GetItem(PyList_GetItem(db_, 0), 13));
	auto l_shipmode= (VarChar<10>*)PyArray_DATA(PyList_GetItem(PyList_GetItem(db_, 0), 14));
	auto l_comment= (VarChar<44>*)PyArray_DATA(PyList_GetItem(PyList_GetItem(db_, 0), 15));
	auto l_NA= (VarChar<1>*)PyArray_DATA(PyList_GetItem(PyList_GetItem(db_, 0), 16));

	auto pa_size = PyArray_Size(PyList_GetItem(PyList_GetItem(db_, 1), 0));
	db->pa_dataset_size = pa_size;
	auto p_partkey = (long*)PyArray_DATA(PyList_GetItem(PyList_GetItem(db_, 1), 0));
	auto p_name= (VarChar<55>*)PyArray_DATA(PyList_GetItem(PyList_GetItem(db_, 1), 1));
	auto p_mfgr= (VarChar<25>*)PyArray_DATA(PyList_GetItem(PyList_GetItem(db_, 1), 2));
	auto p_brand= (VarChar<10>*)PyArray_DATA(PyList_GetItem(PyList_GetItem(db_, 1), 3));
	auto p_type= (VarChar<25>*)PyArray_DATA(PyList_GetItem(PyList_GetItem(db_, 1), 4));
	auto p_size = (long*)PyArray_DATA(PyList_GetItem(PyList_GetItem(db_, 1), 5));
	auto p_container= (VarChar<10>*)PyArray_DATA(PyList_GetItem(PyList_GetItem(db_, 1), 6));
	auto p_retailprice = (double*)PyArray_DATA(PyList_GetItem(PyList_GetItem(db_, 1), 7));
	auto p_comment= (VarChar<23>*)PyArray_DATA(PyList_GetItem(PyList_GetItem(db_, 1), 8));
	auto p_NA= (VarChar<1>*)PyArray_DATA(PyList_GetItem(PyList_GetItem(db_, 1), 9));



	
auto v1151 = phmap::flat_hash_map<tuple<long,long,long,long,double,double,double,double,VarChar<1>,VarChar<1>,long,long,long,VarChar<25>,VarChar<10>,VarChar<44>,VarChar<1>>,bool>({});
tbb::enumerable_thread_specific<phmap::flat_hash_map<tuple<long,long,long,long,double,double,double,double,VarChar<1>,VarChar<1>,long,long,long,VarChar<25>,VarChar<10>,VarChar<44>,VarChar<1>>,bool>> v1168;
auto v1153 = phmap::flat_hash_map<long,phmap::flat_hash_map<tuple<long,VarChar<55>,VarChar<25>,VarChar<10>,VarChar<25>,long,VarChar<10>,double,VarChar<23>,VarChar<1>>,bool>>({});
tbb::enumerable_thread_specific<phmap::flat_hash_map<long,phmap::flat_hash_map<tuple<long,VarChar<55>,VarChar<25>,VarChar<10>,VarChar<25>,long,VarChar<10>,double,VarChar<23>,VarChar<1>>,bool>>> v1175;
auto v1157 = phmap::flat_hash_map<tuple<long,long,long,long,double,double,double,double,VarChar<1>,VarChar<1>,long,long,long,VarChar<25>,VarChar<10>,VarChar<44>,VarChar<1>,long,VarChar<55>,VarChar<25>,VarChar<10>,VarChar<25>,long,VarChar<10>,double,VarChar<23>,VarChar<1>>,bool>({});
auto v1159 = phmap::flat_hash_map<tuple<long,long,long,long,double,double,double,double,VarChar<1>,VarChar<1>,long,long,long,VarChar<25>,VarChar<10>,VarChar<44>,VarChar<1>,long,VarChar<55>,VarChar<25>,VarChar<10>,VarChar<25>,long,VarChar<10>,double,VarChar<23>,VarChar<1>,double>,bool>({});
auto v1161 = phmap::flat_hash_map<tuple<long,long,long,long,double,double,double,double,VarChar<1>,VarChar<1>,long,long,long,VarChar<25>,VarChar<10>,VarChar<44>,VarChar<1>,long,VarChar<55>,VarChar<25>,VarChar<10>,VarChar<25>,long,VarChar<10>,double,VarChar<23>,VarChar<1>,double,double>,bool>({});
auto v1163 = make_tuple(0.0,0.0);	
	const auto& promo = ConstantString("PROMO", 6);

auto v1164 = db->li_dataset_size; tbb::parallel_for(tbb::blocked_range<size_t>(0, v1164), [&](const tbb::blocked_range<size_t>& v1165){auto& v1169=v1168.local();for (size_t v1150=v1165.begin(), end=v1165.end(); v1150!=end; ++v1150){if (((l_shipdate[v1150] >= (long)19950901) && (l_shipdate[v1150] < (long)19951001))){v1169[make_tuple(l_orderkey[v1150], l_partkey[v1150], l_suppkey[v1150], l_linenumber[v1150], l_quantity[v1150], l_extendedprice[v1150], l_discount[v1150], l_tax[v1150], l_returnflag[v1150], l_linestatus[v1150], l_shipdate[v1150], l_commitdate[v1150], l_receiptdate[v1150], l_shipinstruct[v1150], l_shipmode[v1150], l_comment[v1150], l_NA[v1150])] += true;};}});for (auto& local : v1168)AddMap<phmap::flat_hash_map<tuple<long,long,long,long,double,double,double,double,VarChar<1>,VarChar<1>,long,long,long,VarChar<25>,VarChar<10>,VarChar<44>,VarChar<1>>,bool>,tuple<long,long,long,long,double,double,double,double,VarChar<1>,VarChar<1>,long,long,long,VarChar<25>,VarChar<10>,VarChar<44>,VarChar<1>>,bool>(v1151, local);const auto& part_lineitem_probe_pre_ops = v1151;

auto v1171 = db->pa_dataset_size; tbb::parallel_for(tbb::blocked_range<size_t>(0, v1171), [&](const tbb::blocked_range<size_t>& v1172){auto& v1176=v1175.local();for (size_t v1152=v1172.begin(), end=v1172.end(); v1152!=end; ++v1152){v1176[p_partkey[v1152]] += phmap::flat_hash_map<tuple<long,VarChar<55>,VarChar<25>,VarChar<10>,VarChar<25>,long,VarChar<10>,double,VarChar<23>,VarChar<1>>,bool>({{make_tuple(p_partkey[v1152], p_name[v1152], p_mfgr[v1152], p_brand[v1152], p_type[v1152], p_size[v1152], p_container[v1152], p_retailprice[v1152], p_comment[v1152], p_NA[v1152]), true}});}});for (auto& local : v1175)AddMap<phmap::flat_hash_map<long,phmap::flat_hash_map<tuple<long,VarChar<55>,VarChar<25>,VarChar<10>,VarChar<25>,long,VarChar<10>,double,VarChar<23>,VarChar<1>>,bool>>,long,phmap::flat_hash_map<tuple<long,VarChar<55>,VarChar<25>,VarChar<10>,VarChar<25>,long,VarChar<10>,double,VarChar<23>,VarChar<1>>,bool>>(v1153, local);const auto& part_lineitem_build_nest_dict = v1153;

for (auto& v1154 : part_lineitem_probe_pre_ops){if (((part_lineitem_build_nest_dict).contains( /* l_partkey */get<1>((v1154.first))))){for (auto& v1155 : (part_lineitem_build_nest_dict).at( /* l_partkey */get<1>((v1154.first)))){v1157[tuple_cat((v1154.first),move((v1155.first)))] += true;};};}const auto& part_lineitem_0 = v1157;

for (auto& v1158 : part_lineitem_0){v1159[tuple_cat((v1158.first),move(make_tuple((( /* p_type */get<21>((v1158.first)).startsWith(promo))) ? (( /* l_extendedprice */get<5>((v1158.first)) * (1.0 -  /* l_discount */get<6>((v1158.first))))) : (0.0))))] += (v1158.second);}const auto& part_lineitem_1 = v1159;

for (auto& v1160 : part_lineitem_1){v1161[tuple_cat((v1160.first),move(make_tuple(( /* l_extendedprice */get<5>((v1160.first)) * (1.0 -  /* l_discount */get<6>((v1160.first)))))))] += (v1160.second);}const auto& part_lineitem_2 = v1161;

for (auto& v1162 : part_lineitem_2){make_tuple( /* A */get<27>((v1162.first)), /* B */get<28>((v1162.first)));}const auto& JQ_JQ_A_mul_1000_XZ_div_B_XZ_pre_ops = v1163;const auto& results = (( /* A_sum */get<0>(JQ_JQ_A_mul_1000_XZ_div_B_XZ_pre_ops) * 100.0) /  /* B_sum */get<1>(JQ_JQ_A_mul_1000_XZ_div_B_XZ_pre_ops));const auto& out = results;

	return PyFloat_FromDouble(out);

}

static PyObject * q15(PyObject *self, PyObject* args)
{
	

    PyObject * db_;
    if (!PyArg_ParseTuple(args, "O", &db_)){
        PyErr_SetString(PyExc_ValueError,"Error while parsing the sum inputs.");
        return NULL;
    }
    
    
    DB dbobj;
    DB* db = &dbobj;
    
const static int numpy_initialized =  init_numpy();


	auto li_size = PyArray_Size(PyList_GetItem(PyList_GetItem(db_, 0), 0));
	db->li_dataset_size = li_size;
	auto l_orderkey = (long*)PyArray_DATA(PyList_GetItem(PyList_GetItem(db_, 0), 0));
	auto l_partkey = (long*)PyArray_DATA(PyList_GetItem(PyList_GetItem(db_, 0), 1));
	auto l_suppkey = (long*)PyArray_DATA(PyList_GetItem(PyList_GetItem(db_, 0), 2));
	auto l_linenumber = (long*)PyArray_DATA(PyList_GetItem(PyList_GetItem(db_, 0), 3));
	auto l_quantity = (double*)PyArray_DATA(PyList_GetItem(PyList_GetItem(db_, 0), 4));
	auto l_extendedprice = (double*)PyArray_DATA(PyList_GetItem(PyList_GetItem(db_, 0), 5));
	auto l_discount = (double*)PyArray_DATA(PyList_GetItem(PyList_GetItem(db_, 0), 6));
	auto l_tax = (double*)PyArray_DATA(PyList_GetItem(PyList_GetItem(db_, 0), 7));
	auto l_returnflag= (VarChar<1>*)PyArray_DATA(PyList_GetItem(PyList_GetItem(db_, 0), 8));
	auto l_linestatus= (VarChar<1>*)PyArray_DATA(PyList_GetItem(PyList_GetItem(db_, 0), 9));
	auto l_shipdate = (long*)PyArray_DATA(PyList_GetItem(PyList_GetItem(db_, 0), 10));
	auto l_commitdate = (long*)PyArray_DATA(PyList_GetItem(PyList_GetItem(db_, 0), 11));
	auto l_receiptdate = (long*)PyArray_DATA(PyList_GetItem(PyList_GetItem(db_, 0), 12));
	auto l_shipinstruct= (VarChar<25>*)PyArray_DATA(PyList_GetItem(PyList_GetItem(db_, 0), 13));
	auto l_shipmode= (VarChar<10>*)PyArray_DATA(PyList_GetItem(PyList_GetItem(db_, 0), 14));
	auto l_comment= (VarChar<44>*)PyArray_DATA(PyList_GetItem(PyList_GetItem(db_, 0), 15));
	auto l_NA= (VarChar<1>*)PyArray_DATA(PyList_GetItem(PyList_GetItem(db_, 0), 16));

	auto su_size = PyArray_Size(PyList_GetItem(PyList_GetItem(db_, 1), 0));
	db->su_dataset_size = su_size;
	auto s_suppkey = (long*)PyArray_DATA(PyList_GetItem(PyList_GetItem(db_, 1), 0));
	auto s_name= (VarChar<25>*)PyArray_DATA(PyList_GetItem(PyList_GetItem(db_, 1), 1));
	auto s_address= (VarChar<40>*)PyArray_DATA(PyList_GetItem(PyList_GetItem(db_, 1), 2));
	auto s_nationkey = (long*)PyArray_DATA(PyList_GetItem(PyList_GetItem(db_, 1), 3));
	auto s_phone= (VarChar<15>*)PyArray_DATA(PyList_GetItem(PyList_GetItem(db_, 1), 4));
	auto s_acctbal = (double*)PyArray_DATA(PyList_GetItem(PyList_GetItem(db_, 1), 5));
	auto s_comment= (VarChar<101>*)PyArray_DATA(PyList_GetItem(PyList_GetItem(db_, 1), 6));
	auto s_NA= (VarChar<1>*)PyArray_DATA(PyList_GetItem(PyList_GetItem(db_, 1), 7));



	
auto v1199 = phmap::flat_hash_map<tuple<long,long,long,long,double,double,double,double,VarChar<1>,VarChar<1>,long,long,long,VarChar<25>,VarChar<10>,VarChar<44>,VarChar<1>>,bool>({});
tbb::enumerable_thread_specific<phmap::flat_hash_map<tuple<long,long,long,long,double,double,double,double,VarChar<1>,VarChar<1>,long,long,long,VarChar<25>,VarChar<10>,VarChar<44>,VarChar<1>>,bool>> v1222;
auto v1201 = phmap::flat_hash_map<tuple<long,long,long,long,double,double,double,double,VarChar<1>,VarChar<1>,long,long,long,VarChar<25>,VarChar<10>,VarChar<44>,VarChar<1>,double>,bool>({});
auto v1203 = phmap::flat_hash_map<tuple<long>,tuple<double>>({});
auto v1205 = phmap::flat_hash_map<tuple<long,double>,bool>({});
auto v1207 = phmap::flat_hash_map<tuple<long,double>,bool>({});
auto v1209 = phmap::flat_hash_map<long,phmap::flat_hash_map<tuple<long,VarChar<25>,VarChar<40>,long,VarChar<15>,double,VarChar<101>,VarChar<1>>,bool>>({});
tbb::enumerable_thread_specific<phmap::flat_hash_map<long,phmap::flat_hash_map<tuple<long,VarChar<25>,VarChar<40>,long,VarChar<15>,double,VarChar<101>,VarChar<1>>,bool>>> v1229;
auto v1213 = phmap::flat_hash_map<tuple<long,double,long,VarChar<25>,VarChar<40>,long,VarChar<15>,double,VarChar<101>,VarChar<1>>,bool>({});
auto v1215 = phmap::flat_hash_map<tuple<long,double,long,VarChar<25>,VarChar<40>,long,VarChar<15>,double,VarChar<101>,VarChar<1>>,phmap::flat_hash_map<tuple<long,VarChar<25>,VarChar<40>,VarChar<15>,double>,bool>>({});
auto v1217 = phmap::flat_hash_map<tuple<long,VarChar<25>,VarChar<40>,VarChar<15>,double>,bool>({});	
	

auto v1218 = db->li_dataset_size; tbb::parallel_for(tbb::blocked_range<size_t>(0, v1218), [&](const tbb::blocked_range<size_t>& v1219){auto& v1223=v1222.local();for (size_t v1198=v1219.begin(), end=v1219.end(); v1198!=end; ++v1198){if (((l_shipdate[v1198] >= (long)19960101) && (l_shipdate[v1198] < (long)19960401))){v1223[make_tuple(l_orderkey[v1198], l_partkey[v1198], l_suppkey[v1198], l_linenumber[v1198], l_quantity[v1198], l_extendedprice[v1198], l_discount[v1198], l_tax[v1198], l_returnflag[v1198], l_linestatus[v1198], l_shipdate[v1198], l_commitdate[v1198], l_receiptdate[v1198], l_shipinstruct[v1198], l_shipmode[v1198], l_comment[v1198], l_NA[v1198])] += true;};}});for (auto& local : v1222)AddMap<phmap::flat_hash_map<tuple<long,long,long,long,double,double,double,double,VarChar<1>,VarChar<1>,long,long,long,VarChar<25>,VarChar<10>,VarChar<44>,VarChar<1>>,bool>,tuple<long,long,long,long,double,double,double,double,VarChar<1>,VarChar<1>,long,long,long,VarChar<25>,VarChar<10>,VarChar<44>,VarChar<1>>,bool>(v1199, local);const auto& lineitem_0 = v1199;

for (auto& v1200 : lineitem_0){v1201[tuple_cat((v1200.first),move(make_tuple(( /* l_extendedprice */get<5>((v1200.first)) * (1.0 -  /* l_discount */get<6>((v1200.first)))))))] += (v1200.second);}const auto& lineitem_1 = v1201;

for (auto& v1202 : lineitem_1){v1203[make_tuple( /* l_suppkey */get<2>((v1202.first)))] += make_tuple( /* revenue */get<17>((v1202.first)));}const auto& lineitem_2 = v1203;

for (auto& v1204 : lineitem_2){v1205[tuple_cat((v1204.first),move((v1204.second)))] += true;}const auto& lineitem_3 = v1205;

for (auto& v1206 : lineitem_3){if (( /* total_revenue */get<1>((v1206.first)) == 1772627.2087)){v1207[(v1206.first)] += (v1206.second);};}const auto& supplier_lineitem_probe_pre_ops = v1207;

auto v1225 = db->su_dataset_size; tbb::parallel_for(tbb::blocked_range<size_t>(0, v1225), [&](const tbb::blocked_range<size_t>& v1226){auto& v1230=v1229.local();for (size_t v1208=v1226.begin(), end=v1226.end(); v1208!=end; ++v1208){v1230[s_suppkey[v1208]] += phmap::flat_hash_map<tuple<long,VarChar<25>,VarChar<40>,long,VarChar<15>,double,VarChar<101>,VarChar<1>>,bool>({{make_tuple(s_suppkey[v1208], s_name[v1208], s_address[v1208], s_nationkey[v1208], s_phone[v1208], s_acctbal[v1208], s_comment[v1208], s_NA[v1208]), true}});}});for (auto& local : v1229)AddMap<phmap::flat_hash_map<long,phmap::flat_hash_map<tuple<long,VarChar<25>,VarChar<40>,long,VarChar<15>,double,VarChar<101>,VarChar<1>>,bool>>,long,phmap::flat_hash_map<tuple<long,VarChar<25>,VarChar<40>,long,VarChar<15>,double,VarChar<101>,VarChar<1>>,bool>>(v1209, local);const auto& supplier_lineitem_build_nest_dict = v1209;

for (auto& v1210 : supplier_lineitem_probe_pre_ops){if (((supplier_lineitem_build_nest_dict).contains( /* l_suppkey */get<0>((v1210.first))))){for (auto& v1211 : (supplier_lineitem_build_nest_dict).at( /* l_suppkey */get<0>((v1210.first)))){v1213[tuple_cat((v1210.first),move((v1211.first)))] += true;};};}const auto& supplier_lineitem_0 = v1213;

for (auto& v1214 : supplier_lineitem_0){v1215[(v1214.first)] += phmap::flat_hash_map<tuple<long,VarChar<25>,VarChar<40>,VarChar<15>,double>,bool>({{make_tuple( /* s_suppkey */get<2>((v1214.first)), /* s_name */get<3>((v1214.first)), /* s_address */get<4>((v1214.first)), /* s_phone */get<6>((v1214.first)), /* total_revenue */get<1>((v1214.first))), true}});}const auto& supplier_lineitem_1 = v1215;

for (auto& v1216 : supplier_lineitem_1){(v1216.second);}const auto& results = v1217;const auto& out = results;

    FastDict_is25s40s15f_b* result = (FastDict_is25s40s15f_b*)PyObject_CallObject(PyObject_CallObject(PyObject_GetAttrString(PyImport_Import(PyUnicode_FromString("test_unopt_fastdict_compiled")), (char*)"new_FastDict_is25s40s15f_b"), nullptr), nullptr);
    *(result->dict) = out;

    return (PyObject*)PyObject_CallObject(PyObject_GetAttrString(PyImport_Import(PyUnicode_FromString("sdqlpy.fastd")), "fastd"), Py_BuildValue("(OO)", result, PyUnicode_FromString("test_unopt")));

}

static PyObject * q16(PyObject *self, PyObject* args)
{
	

    PyObject * db_;
    if (!PyArg_ParseTuple(args, "O", &db_)){
        PyErr_SetString(PyExc_ValueError,"Error while parsing the sum inputs.");
        return NULL;
    }
    
    
    DB dbobj;
    DB* db = &dbobj;
    
const static int numpy_initialized =  init_numpy();


	auto ps_size = PyArray_Size(PyList_GetItem(PyList_GetItem(db_, 0), 0));
	db->ps_dataset_size = ps_size;
	auto ps_partkey = (long*)PyArray_DATA(PyList_GetItem(PyList_GetItem(db_, 0), 0));
	auto ps_suppkey = (long*)PyArray_DATA(PyList_GetItem(PyList_GetItem(db_, 0), 1));
	auto ps_availqty = (double*)PyArray_DATA(PyList_GetItem(PyList_GetItem(db_, 0), 2));
	auto ps_supplycost = (double*)PyArray_DATA(PyList_GetItem(PyList_GetItem(db_, 0), 3));
	auto ps_comment= (VarChar<199>*)PyArray_DATA(PyList_GetItem(PyList_GetItem(db_, 0), 4));
	auto ps_NA= (VarChar<1>*)PyArray_DATA(PyList_GetItem(PyList_GetItem(db_, 0), 5));

	auto pa_size = PyArray_Size(PyList_GetItem(PyList_GetItem(db_, 1), 0));
	db->pa_dataset_size = pa_size;
	auto p_partkey = (long*)PyArray_DATA(PyList_GetItem(PyList_GetItem(db_, 1), 0));
	auto p_name= (VarChar<55>*)PyArray_DATA(PyList_GetItem(PyList_GetItem(db_, 1), 1));
	auto p_mfgr= (VarChar<25>*)PyArray_DATA(PyList_GetItem(PyList_GetItem(db_, 1), 2));
	auto p_brand= (VarChar<10>*)PyArray_DATA(PyList_GetItem(PyList_GetItem(db_, 1), 3));
	auto p_type= (VarChar<25>*)PyArray_DATA(PyList_GetItem(PyList_GetItem(db_, 1), 4));
	auto p_size = (long*)PyArray_DATA(PyList_GetItem(PyList_GetItem(db_, 1), 5));
	auto p_container= (VarChar<10>*)PyArray_DATA(PyList_GetItem(PyList_GetItem(db_, 1), 6));
	auto p_retailprice = (double*)PyArray_DATA(PyList_GetItem(PyList_GetItem(db_, 1), 7));
	auto p_comment= (VarChar<23>*)PyArray_DATA(PyList_GetItem(PyList_GetItem(db_, 1), 8));
	auto p_NA= (VarChar<1>*)PyArray_DATA(PyList_GetItem(PyList_GetItem(db_, 1), 9));

	auto su_size = PyArray_Size(PyList_GetItem(PyList_GetItem(db_, 2), 0));
	db->su_dataset_size = su_size;
	auto s_suppkey = (long*)PyArray_DATA(PyList_GetItem(PyList_GetItem(db_, 2), 0));
	auto s_name= (VarChar<25>*)PyArray_DATA(PyList_GetItem(PyList_GetItem(db_, 2), 1));
	auto s_address= (VarChar<40>*)PyArray_DATA(PyList_GetItem(PyList_GetItem(db_, 2), 2));
	auto s_nationkey = (long*)PyArray_DATA(PyList_GetItem(PyList_GetItem(db_, 2), 3));
	auto s_phone= (VarChar<15>*)PyArray_DATA(PyList_GetItem(PyList_GetItem(db_, 2), 4));
	auto s_acctbal = (double*)PyArray_DATA(PyList_GetItem(PyList_GetItem(db_, 2), 5));
	auto s_comment= (VarChar<101>*)PyArray_DATA(PyList_GetItem(PyList_GetItem(db_, 2), 6));
	auto s_NA= (VarChar<1>*)PyArray_DATA(PyList_GetItem(PyList_GetItem(db_, 2), 7));



	
auto v1251 = phmap::flat_hash_map<tuple<long,VarChar<55>,VarChar<25>,VarChar<10>,VarChar<25>,long,VarChar<10>,double,VarChar<23>,VarChar<1>>,bool>({});
tbb::enumerable_thread_specific<phmap::flat_hash_map<tuple<long,VarChar<55>,VarChar<25>,VarChar<10>,VarChar<25>,long,VarChar<10>,double,VarChar<23>,VarChar<1>>,bool>> v1272;
auto v1253 = phmap::flat_hash_map<tuple<long,VarChar<25>,VarChar<40>,long,VarChar<15>,double,VarChar<101>,VarChar<1>>,bool>({});
tbb::enumerable_thread_specific<phmap::flat_hash_map<tuple<long,VarChar<25>,VarChar<40>,long,VarChar<15>,double,VarChar<101>,VarChar<1>>,bool>> v1279;
auto v1255 = phmap::flat_hash_map<long,bool>({});
auto v1257 = phmap::flat_hash_map<tuple<long,long,double,double,VarChar<199>,VarChar<1>>,bool>({});
tbb::enumerable_thread_specific<phmap::flat_hash_map<tuple<long,long,double,double,VarChar<199>,VarChar<1>>,bool>> v1286;
auto v1259 = phmap::flat_hash_map<long,phmap::flat_hash_map<tuple<long,VarChar<55>,VarChar<25>,VarChar<10>,VarChar<25>,long,VarChar<10>,double,VarChar<23>,VarChar<1>>,bool>>({});
auto v1263 = phmap::flat_hash_map<tuple<long,long,double,double,VarChar<199>,VarChar<1>,long,VarChar<55>,VarChar<25>,VarChar<10>,VarChar<25>,long,VarChar<10>,double,VarChar<23>,VarChar<1>>,bool>({});
auto v1265 = phmap::flat_hash_map<tuple<VarChar<10>,VarChar<25>,long>,tuple<phmap::flat_hash_map<long,bool>>>({});
auto v1267 = phmap::flat_hash_map<tuple<VarChar<10>,VarChar<25>,long,long>,bool>({});	
	const auto& brand45 = ConstantString("Brand#45", 9);const auto& mediumpolished = ConstantString("MEDIUM POLISHED", 16);const auto& customer = ConstantString("Customer", 9);const auto& complaints = ConstantString("Complaints", 11);

auto v1268 = db->pa_dataset_size; tbb::parallel_for(tbb::blocked_range<size_t>(0, v1268), [&](const tbb::blocked_range<size_t>& v1269){auto& v1273=v1272.local();for (size_t v1250=v1269.begin(), end=v1269.end(); v1250!=end; ++v1250){if ((((p_brand[v1250] != brand45) && ((p_type[v1250].startsWith(mediumpolished)) == false)) && ((((((((p_size[v1250] == (long)9) || (p_size[v1250] == (long)36)) || (p_size[v1250] == (long)49)) || (p_size[v1250] == (long)14)) || (p_size[v1250] == (long)23)) || (p_size[v1250] == (long)45)) || (p_size[v1250] == (long)19)) || (p_size[v1250] == (long)3)))){v1273[make_tuple(p_partkey[v1250], p_name[v1250], p_mfgr[v1250], p_brand[v1250], p_type[v1250], p_size[v1250], p_container[v1250], p_retailprice[v1250], p_comment[v1250], p_NA[v1250])] += true;};}});for (auto& local : v1272)AddMap<phmap::flat_hash_map<tuple<long,VarChar<55>,VarChar<25>,VarChar<10>,VarChar<25>,long,VarChar<10>,double,VarChar<23>,VarChar<1>>,bool>,tuple<long,VarChar<55>,VarChar<25>,VarChar<10>,VarChar<25>,long,VarChar<10>,double,VarChar<23>,VarChar<1>>,bool>(v1251, local);const auto& part_partsupp_build_pre_ops = v1251;

auto v1275 = db->su_dataset_size; tbb::parallel_for(tbb::blocked_range<size_t>(0, v1275), [&](const tbb::blocked_range<size_t>& v1276){auto& v1280=v1279.local();for (size_t v1252=v1276.begin(), end=v1276.end(); v1252!=end; ++v1252){if ((((s_comment[v1252].firstIndex(customer)) != (((long)-1 * (long)1) * (long)1)) && ((s_comment[v1252].firstIndex(complaints)) > ((s_comment[v1252].firstIndex(customer)) + (long)7)))){v1280[make_tuple(s_suppkey[v1252], s_name[v1252], s_address[v1252], s_nationkey[v1252], s_phone[v1252], s_acctbal[v1252], s_comment[v1252], s_NA[v1252])] += true;};}});for (auto& local : v1279)AddMap<phmap::flat_hash_map<tuple<long,VarChar<25>,VarChar<40>,long,VarChar<15>,double,VarChar<101>,VarChar<1>>,bool>,tuple<long,VarChar<25>,VarChar<40>,long,VarChar<15>,double,VarChar<101>,VarChar<1>>,bool>(v1253, local);const auto& supplier_partsupp_isin_pre_ops = v1253;

for (auto& v1254 : supplier_partsupp_isin_pre_ops){v1255[ /* s_suppkey */get<0>((v1254.first))] += true;}const auto& supplier_partsupp_isin_build_index = v1255;

auto v1282 = db->ps_dataset_size; tbb::parallel_for(tbb::blocked_range<size_t>(0, v1282), [&](const tbb::blocked_range<size_t>& v1283){auto& v1287=v1286.local();for (size_t v1256=v1283.begin(), end=v1283.end(); v1256!=end; ++v1256){if ((!((supplier_partsupp_isin_build_index).contains(ps_suppkey[v1256])))){v1287[make_tuple(ps_partkey[v1256], ps_suppkey[v1256], ps_availqty[v1256], ps_supplycost[v1256], ps_comment[v1256], ps_NA[v1256])] += true;};}});for (auto& local : v1286)AddMap<phmap::flat_hash_map<tuple<long,long,double,double,VarChar<199>,VarChar<1>>,bool>,tuple<long,long,double,double,VarChar<199>,VarChar<1>>,bool>(v1257, local);const auto& part_partsupp_probe_pre_ops = v1257;

for (auto& v1258 : part_partsupp_build_pre_ops){v1259[ /* p_partkey */get<0>((v1258.first))] += phmap::flat_hash_map<tuple<long,VarChar<55>,VarChar<25>,VarChar<10>,VarChar<25>,long,VarChar<10>,double,VarChar<23>,VarChar<1>>,bool>({{(v1258.first), (v1258.second)}});}const auto& part_partsupp_build_nest_dict = v1259;

for (auto& v1260 : part_partsupp_probe_pre_ops){if (((part_partsupp_build_nest_dict).contains( /* ps_partkey */get<0>((v1260.first))))){for (auto& v1261 : (part_partsupp_build_nest_dict).at( /* ps_partkey */get<0>((v1260.first)))){v1263[tuple_cat((v1260.first),move((v1261.first)))] += true;};};}const auto& part_partsupp_0 = v1263;

for (auto& v1264 : part_partsupp_0){v1265[make_tuple( /* p_brand */get<9>((v1264.first)), /* p_type */get<10>((v1264.first)), /* p_size */get<11>((v1264.first)))] += make_tuple(phmap::flat_hash_map<long,bool>({{ /* ps_suppkey */get<1>((v1264.first)), true}}));}const auto& part_partsupp_1 = v1265;

for (auto& v1266 : part_partsupp_1){v1267[make_tuple( /* p_brand */get<0>((v1266.first)), /* p_type */get<1>((v1266.first)), /* p_size */get<2>((v1266.first)),( /* supplier_cnt */get<0>((v1266.second)).size()))] += true;}const auto& results = v1267;const auto& out = results;

    FastDict_s10s25ii_b* result = (FastDict_s10s25ii_b*)PyObject_CallObject(PyObject_CallObject(PyObject_GetAttrString(PyImport_Import(PyUnicode_FromString("test_unopt_fastdict_compiled")), (char*)"new_FastDict_s10s25ii_b"), nullptr), nullptr);
    *(result->dict) = out;

    return (PyObject*)PyObject_CallObject(PyObject_GetAttrString(PyImport_Import(PyUnicode_FromString("sdqlpy.fastd")), "fastd"), Py_BuildValue("(OO)", result, PyUnicode_FromString("test_unopt")));

}

static PyObject * q17(PyObject *self, PyObject* args)
{
	

    PyObject * db_;
    if (!PyArg_ParseTuple(args, "O", &db_)){
        PyErr_SetString(PyExc_ValueError,"Error while parsing the sum inputs.");
        return NULL;
    }
    
    
    DB dbobj;
    DB* db = &dbobj;
    
const static int numpy_initialized =  init_numpy();


	auto li_size = PyArray_Size(PyList_GetItem(PyList_GetItem(db_, 0), 0));
	db->li_dataset_size = li_size;
	auto l_orderkey = (long*)PyArray_DATA(PyList_GetItem(PyList_GetItem(db_, 0), 0));
	auto l_partkey = (long*)PyArray_DATA(PyList_GetItem(PyList_GetItem(db_, 0), 1));
	auto l_suppkey = (long*)PyArray_DATA(PyList_GetItem(PyList_GetItem(db_, 0), 2));
	auto l_linenumber = (long*)PyArray_DATA(PyList_GetItem(PyList_GetItem(db_, 0), 3));
	auto l_quantity = (double*)PyArray_DATA(PyList_GetItem(PyList_GetItem(db_, 0), 4));
	auto l_extendedprice = (double*)PyArray_DATA(PyList_GetItem(PyList_GetItem(db_, 0), 5));
	auto l_discount = (double*)PyArray_DATA(PyList_GetItem(PyList_GetItem(db_, 0), 6));
	auto l_tax = (double*)PyArray_DATA(PyList_GetItem(PyList_GetItem(db_, 0), 7));
	auto l_returnflag= (VarChar<1>*)PyArray_DATA(PyList_GetItem(PyList_GetItem(db_, 0), 8));
	auto l_linestatus= (VarChar<1>*)PyArray_DATA(PyList_GetItem(PyList_GetItem(db_, 0), 9));
	auto l_shipdate = (long*)PyArray_DATA(PyList_GetItem(PyList_GetItem(db_, 0), 10));
	auto l_commitdate = (long*)PyArray_DATA(PyList_GetItem(PyList_GetItem(db_, 0), 11));
	auto l_receiptdate = (long*)PyArray_DATA(PyList_GetItem(PyList_GetItem(db_, 0), 12));
	auto l_shipinstruct= (VarChar<25>*)PyArray_DATA(PyList_GetItem(PyList_GetItem(db_, 0), 13));
	auto l_shipmode= (VarChar<10>*)PyArray_DATA(PyList_GetItem(PyList_GetItem(db_, 0), 14));
	auto l_comment= (VarChar<44>*)PyArray_DATA(PyList_GetItem(PyList_GetItem(db_, 0), 15));
	auto l_NA= (VarChar<1>*)PyArray_DATA(PyList_GetItem(PyList_GetItem(db_, 0), 16));

	auto pa_size = PyArray_Size(PyList_GetItem(PyList_GetItem(db_, 1), 0));
	db->pa_dataset_size = pa_size;
	auto p_partkey = (long*)PyArray_DATA(PyList_GetItem(PyList_GetItem(db_, 1), 0));
	auto p_name= (VarChar<55>*)PyArray_DATA(PyList_GetItem(PyList_GetItem(db_, 1), 1));
	auto p_mfgr= (VarChar<25>*)PyArray_DATA(PyList_GetItem(PyList_GetItem(db_, 1), 2));
	auto p_brand= (VarChar<10>*)PyArray_DATA(PyList_GetItem(PyList_GetItem(db_, 1), 3));
	auto p_type= (VarChar<25>*)PyArray_DATA(PyList_GetItem(PyList_GetItem(db_, 1), 4));
	auto p_size = (long*)PyArray_DATA(PyList_GetItem(PyList_GetItem(db_, 1), 5));
	auto p_container= (VarChar<10>*)PyArray_DATA(PyList_GetItem(PyList_GetItem(db_, 1), 6));
	auto p_retailprice = (double*)PyArray_DATA(PyList_GetItem(PyList_GetItem(db_, 1), 7));
	auto p_comment= (VarChar<23>*)PyArray_DATA(PyList_GetItem(PyList_GetItem(db_, 1), 8));
	auto p_NA= (VarChar<1>*)PyArray_DATA(PyList_GetItem(PyList_GetItem(db_, 1), 9));



	
auto v1312 = phmap::flat_hash_map<tuple<long,VarChar<55>,VarChar<25>,VarChar<10>,VarChar<25>,long,VarChar<10>,double,VarChar<23>,VarChar<1>>,bool>({});
tbb::enumerable_thread_specific<phmap::flat_hash_map<tuple<long,VarChar<55>,VarChar<25>,VarChar<10>,VarChar<25>,long,VarChar<10>,double,VarChar<23>,VarChar<1>>,bool>> v1337;
auto v1314 = phmap::flat_hash_map<tuple<long>,tuple<double,double>>({});
tbb::enumerable_thread_specific<phmap::flat_hash_map<tuple<long>,tuple<double,double>>> v1344;
auto v1316 = phmap::flat_hash_map<tuple<long,double,double>,bool>({});
auto v1318 = phmap::flat_hash_map<long,phmap::flat_hash_map<tuple<long,VarChar<55>,VarChar<25>,VarChar<10>,VarChar<25>,long,VarChar<10>,double,VarChar<23>,VarChar<1>>,bool>>({});
auto v1322 = phmap::flat_hash_map<tuple<long,double,double,long,VarChar<55>,VarChar<25>,VarChar<10>,VarChar<25>,long,VarChar<10>,double,VarChar<23>,VarChar<1>>,bool>({});
auto v1324 = phmap::flat_hash_map<long,phmap::flat_hash_map<tuple<long,double,double,long,VarChar<55>,VarChar<25>,VarChar<10>,VarChar<25>,long,VarChar<10>,double,VarChar<23>,VarChar<1>>,bool>>({});
auto v1328 = phmap::flat_hash_map<tuple<long,long,long,long,double,double,double,double,VarChar<1>,VarChar<1>,long,long,long,VarChar<25>,VarChar<10>,VarChar<44>,VarChar<1>,long,double,double,long,VarChar<55>,VarChar<25>,VarChar<10>,VarChar<25>,long,VarChar<10>,double,VarChar<23>,VarChar<1>>,bool>({});
tbb::enumerable_thread_specific<phmap::flat_hash_map<tuple<long,long,long,long,double,double,double,double,VarChar<1>,VarChar<1>,long,long,long,VarChar<25>,VarChar<10>,VarChar<44>,VarChar<1>,long,double,double,long,VarChar<55>,VarChar<25>,VarChar<10>,VarChar<25>,long,VarChar<10>,double,VarChar<23>,VarChar<1>>,bool>> v1351;
auto v1330 = phmap::flat_hash_map<tuple<long,long,long,long,double,double,double,double,VarChar<1>,VarChar<1>,long,long,long,VarChar<25>,VarChar<10>,VarChar<44>,VarChar<1>,long,double,double,long,VarChar<55>,VarChar<25>,VarChar<10>,VarChar<25>,long,VarChar<10>,double,VarChar<23>,VarChar<1>,double>,bool>({});
auto v1332 = make_tuple(0.0);	
	const auto& brand23 = ConstantString("Brand#23", 9);const auto& medbox = ConstantString("MED BOX", 8);

auto v1333 = db->pa_dataset_size; tbb::parallel_for(tbb::blocked_range<size_t>(0, v1333), [&](const tbb::blocked_range<size_t>& v1334){auto& v1338=v1337.local();for (size_t v1311=v1334.begin(), end=v1334.end(); v1311!=end; ++v1311){if (((p_brand[v1311] == brand23) && (p_container[v1311] == medbox))){v1338[make_tuple(p_partkey[v1311], p_name[v1311], p_mfgr[v1311], p_brand[v1311], p_type[v1311], p_size[v1311], p_container[v1311], p_retailprice[v1311], p_comment[v1311], p_NA[v1311])] += true;};}});for (auto& local : v1337)AddMap<phmap::flat_hash_map<tuple<long,VarChar<55>,VarChar<25>,VarChar<10>,VarChar<25>,long,VarChar<10>,double,VarChar<23>,VarChar<1>>,bool>,tuple<long,VarChar<55>,VarChar<25>,VarChar<10>,VarChar<25>,long,VarChar<10>,double,VarChar<23>,VarChar<1>>,bool>(v1312, local);const auto& part_l1_build_pre_ops = v1312;

auto v1340 = db->li_dataset_size; tbb::parallel_for(tbb::blocked_range<size_t>(0, v1340), [&](const tbb::blocked_range<size_t>& v1341){auto& v1345=v1344.local();for (size_t v1313=v1341.begin(), end=v1341.end(); v1313!=end; ++v1313){v1345[make_tuple(l_partkey[v1313])] += make_tuple(l_quantity[v1313],((l_quantity[v1313] != none)) ? (1.0) : (0.0));}});for (auto& local : v1344)AddMap<phmap::flat_hash_map<tuple<long>,tuple<double,double>>,tuple<long>,tuple<double,double>>(v1314, local);const auto& l1_0 = v1314;

for (auto& v1315 : l1_0){v1316[tuple_cat((v1315.first),move((v1315.second)))] += true;}const auto& part_l1_probe_pre_ops = v1316;

for (auto& v1317 : part_l1_build_pre_ops){v1318[ /* p_partkey */get<0>((v1317.first))] += phmap::flat_hash_map<tuple<long,VarChar<55>,VarChar<25>,VarChar<10>,VarChar<25>,long,VarChar<10>,double,VarChar<23>,VarChar<1>>,bool>({{(v1317.first), (v1317.second)}});}const auto& part_l1_build_nest_dict = v1318;

for (auto& v1319 : part_l1_probe_pre_ops){if (((part_l1_build_nest_dict).contains( /* l_partkey */get<0>((v1319.first))))){for (auto& v1320 : (part_l1_build_nest_dict).at( /* l_partkey */get<0>((v1319.first)))){v1322[tuple_cat((v1319.first),move((v1320.first)))] += true;};};}const auto& part_l1_lineitem_build_pre_ops = v1322;

for (auto& v1323 : part_l1_lineitem_build_pre_ops){v1324[ /* l_partkey */get<0>((v1323.first))] += phmap::flat_hash_map<tuple<long,double,double,long,VarChar<55>,VarChar<25>,VarChar<10>,VarChar<25>,long,VarChar<10>,double,VarChar<23>,VarChar<1>>,bool>({{(v1323.first), (v1323.second)}});}const auto& part_l1_lineitem_build_nest_dict = v1324;

auto v1347 = db->li_dataset_size; tbb::parallel_for(tbb::blocked_range<size_t>(0, v1347), [&](const tbb::blocked_range<size_t>& v1348){auto& v1352=v1351.local();for (size_t v1325=v1348.begin(), end=v1348.end(); v1325!=end; ++v1325){if (((part_l1_lineitem_build_nest_dict).contains(l_partkey[v1325]))){for (auto& v1326 : (part_l1_lineitem_build_nest_dict).at(l_partkey[v1325])){v1352[tuple_cat(make_tuple(l_orderkey[v1325], l_partkey[v1325], l_suppkey[v1325], l_linenumber[v1325], l_quantity[v1325], l_extendedprice[v1325], l_discount[v1325], l_tax[v1325], l_returnflag[v1325], l_linestatus[v1325], l_shipdate[v1325], l_commitdate[v1325], l_receiptdate[v1325], l_shipinstruct[v1325], l_shipmode[v1325], l_comment[v1325], l_NA[v1325]),move(make_tuple(l_partkey[v1326], sum_quant[v1326], count_quant[v1326], p_partkey[v1326], p_name[v1326], p_mfgr[v1326], p_brand[v1326], p_type[v1326], p_size[v1326], p_container[v1326], p_retailprice[v1326], p_comment[v1326], p_NA[v1326])))] += true;};};}});for (auto& local : v1351)AddMap<phmap::flat_hash_map<tuple<long,long,long,long,double,double,double,double,VarChar<1>,VarChar<1>,long,long,long,VarChar<25>,VarChar<10>,VarChar<44>,VarChar<1>,long,double,double,long,VarChar<55>,VarChar<25>,VarChar<10>,VarChar<25>,long,VarChar<10>,double,VarChar<23>,VarChar<1>>,bool>,tuple<long,long,long,long,double,double,double,double,VarChar<1>,VarChar<1>,long,long,long,VarChar<25>,VarChar<10>,VarChar<44>,VarChar<1>,long,double,double,long,VarChar<55>,VarChar<25>,VarChar<10>,VarChar<25>,long,VarChar<10>,double,VarChar<23>,VarChar<1>>,bool>(v1328, local);const auto& part_l1_lineitem_0 = v1328;

for (auto& v1329 : part_l1_lineitem_0){v1330[tuple_cat((v1329.first),move(make_tuple((( /* l_quantity */get<4>((v1329.first)) < (0.2 * ( /* sum_quant */get<18>((v1329.first)) /  /* count_quant */get<19>((v1329.first)))))) ? ( /* l_extendedprice */get<5>((v1329.first))) : (0.0))))] += (v1329.second);}const auto& part_l1_lineitem_1 = v1330;

for (auto& v1331 : part_l1_lineitem_1){make_tuple( /* price */get<30>((v1331.first)));}const auto& JQ_price_div_70_XZ_pre_ops = v1332;const auto& results = ( /* price_sum */get<0>(JQ_price_div_70_XZ_pre_ops) / 7.0);const auto& out = results;

	return PyFloat_FromDouble(out);

}

static PyObject * q18(PyObject *self, PyObject* args)
{
	

    PyObject * db_;
    if (!PyArg_ParseTuple(args, "O", &db_)){
        PyErr_SetString(PyExc_ValueError,"Error while parsing the sum inputs.");
        return NULL;
    }
    
    
    DB dbobj;
    DB* db = &dbobj;
    
const static int numpy_initialized =  init_numpy();


	auto li_size = PyArray_Size(PyList_GetItem(PyList_GetItem(db_, 0), 0));
	db->li_dataset_size = li_size;
	auto l_orderkey = (long*)PyArray_DATA(PyList_GetItem(PyList_GetItem(db_, 0), 0));
	auto l_partkey = (long*)PyArray_DATA(PyList_GetItem(PyList_GetItem(db_, 0), 1));
	auto l_suppkey = (long*)PyArray_DATA(PyList_GetItem(PyList_GetItem(db_, 0), 2));
	auto l_linenumber = (long*)PyArray_DATA(PyList_GetItem(PyList_GetItem(db_, 0), 3));
	auto l_quantity = (double*)PyArray_DATA(PyList_GetItem(PyList_GetItem(db_, 0), 4));
	auto l_extendedprice = (double*)PyArray_DATA(PyList_GetItem(PyList_GetItem(db_, 0), 5));
	auto l_discount = (double*)PyArray_DATA(PyList_GetItem(PyList_GetItem(db_, 0), 6));
	auto l_tax = (double*)PyArray_DATA(PyList_GetItem(PyList_GetItem(db_, 0), 7));
	auto l_returnflag= (VarChar<1>*)PyArray_DATA(PyList_GetItem(PyList_GetItem(db_, 0), 8));
	auto l_linestatus= (VarChar<1>*)PyArray_DATA(PyList_GetItem(PyList_GetItem(db_, 0), 9));
	auto l_shipdate = (long*)PyArray_DATA(PyList_GetItem(PyList_GetItem(db_, 0), 10));
	auto l_commitdate = (long*)PyArray_DATA(PyList_GetItem(PyList_GetItem(db_, 0), 11));
	auto l_receiptdate = (long*)PyArray_DATA(PyList_GetItem(PyList_GetItem(db_, 0), 12));
	auto l_shipinstruct= (VarChar<25>*)PyArray_DATA(PyList_GetItem(PyList_GetItem(db_, 0), 13));
	auto l_shipmode= (VarChar<10>*)PyArray_DATA(PyList_GetItem(PyList_GetItem(db_, 0), 14));
	auto l_comment= (VarChar<44>*)PyArray_DATA(PyList_GetItem(PyList_GetItem(db_, 0), 15));
	auto l_NA= (VarChar<1>*)PyArray_DATA(PyList_GetItem(PyList_GetItem(db_, 0), 16));

	auto cu_size = PyArray_Size(PyList_GetItem(PyList_GetItem(db_, 1), 0));
	db->cu_dataset_size = cu_size;
	auto c_custkey = (long*)PyArray_DATA(PyList_GetItem(PyList_GetItem(db_, 1), 0));
	auto c_name= (VarChar<25>*)PyArray_DATA(PyList_GetItem(PyList_GetItem(db_, 1), 1));
	auto c_address= (VarChar<40>*)PyArray_DATA(PyList_GetItem(PyList_GetItem(db_, 1), 2));
	auto c_nationkey = (long*)PyArray_DATA(PyList_GetItem(PyList_GetItem(db_, 1), 3));
	auto c_phone= (VarChar<15>*)PyArray_DATA(PyList_GetItem(PyList_GetItem(db_, 1), 4));
	auto c_acctbal = (double*)PyArray_DATA(PyList_GetItem(PyList_GetItem(db_, 1), 5));
	auto c_mktsegment= (VarChar<10>*)PyArray_DATA(PyList_GetItem(PyList_GetItem(db_, 1), 6));
	auto c_comment= (VarChar<117>*)PyArray_DATA(PyList_GetItem(PyList_GetItem(db_, 1), 7));
	auto c_NA= (VarChar<1>*)PyArray_DATA(PyList_GetItem(PyList_GetItem(db_, 1), 8));

	auto ord_size = PyArray_Size(PyList_GetItem(PyList_GetItem(db_, 2), 0));
	db->ord_dataset_size = ord_size;
	auto o_orderkey = (long*)PyArray_DATA(PyList_GetItem(PyList_GetItem(db_, 2), 0));
	auto o_custkey = (long*)PyArray_DATA(PyList_GetItem(PyList_GetItem(db_, 2), 1));
	auto o_orderstatus= (VarChar<1>*)PyArray_DATA(PyList_GetItem(PyList_GetItem(db_, 2), 2));
	auto o_totalprice = (double*)PyArray_DATA(PyList_GetItem(PyList_GetItem(db_, 2), 3));
	auto o_orderdate = (long*)PyArray_DATA(PyList_GetItem(PyList_GetItem(db_, 2), 4));
	auto o_orderpriority= (VarChar<15>*)PyArray_DATA(PyList_GetItem(PyList_GetItem(db_, 2), 5));
	auto o_clerk= (VarChar<15>*)PyArray_DATA(PyList_GetItem(PyList_GetItem(db_, 2), 6));
	auto o_shippriority = (long*)PyArray_DATA(PyList_GetItem(PyList_GetItem(db_, 2), 7));
	auto o_comment= (VarChar<79>*)PyArray_DATA(PyList_GetItem(PyList_GetItem(db_, 2), 8));
	auto o_NA= (VarChar<1>*)PyArray_DATA(PyList_GetItem(PyList_GetItem(db_, 2), 9));



	
auto v1383 = phmap::flat_hash_map<tuple<long>,tuple<double>>({});
tbb::enumerable_thread_specific<phmap::flat_hash_map<tuple<long>,tuple<double>>> v1414;
auto v1385 = phmap::flat_hash_map<tuple<long,double>,bool>({});
auto v1387 = phmap::flat_hash_map<tuple<long,double>,bool>({});
auto v1389 = phmap::flat_hash_map<tuple<long,double>,bool>({});
auto v1391 = phmap::flat_hash_map<long,bool>({});
auto v1393 = phmap::flat_hash_map<tuple<long,long,VarChar<1>,double,long,VarChar<15>,VarChar<15>,long,VarChar<79>,VarChar<1>>,bool>({});
tbb::enumerable_thread_specific<phmap::flat_hash_map<tuple<long,long,VarChar<1>,double,long,VarChar<15>,VarChar<15>,long,VarChar<79>,VarChar<1>>,bool>> v1421;
auto v1395 = phmap::flat_hash_map<long,phmap::flat_hash_map<tuple<long,VarChar<25>,VarChar<40>,long,VarChar<15>,double,VarChar<10>,VarChar<117>,VarChar<1>>,bool>>({});
tbb::enumerable_thread_specific<phmap::flat_hash_map<long,phmap::flat_hash_map<tuple<long,VarChar<25>,VarChar<40>,long,VarChar<15>,double,VarChar<10>,VarChar<117>,VarChar<1>>,bool>>> v1428;
auto v1399 = phmap::flat_hash_map<tuple<long,long,VarChar<1>,double,long,VarChar<15>,VarChar<15>,long,VarChar<79>,VarChar<1>,long,VarChar<25>,VarChar<40>,long,VarChar<15>,double,VarChar<10>,VarChar<117>,VarChar<1>>,bool>({});
auto v1401 = phmap::flat_hash_map<long,phmap::flat_hash_map<tuple<long,long,VarChar<1>,double,long,VarChar<15>,VarChar<15>,long,VarChar<79>,VarChar<1>,long,VarChar<25>,VarChar<40>,long,VarChar<15>,double,VarChar<10>,VarChar<117>,VarChar<1>>,bool>>({});
auto v1405 = phmap::flat_hash_map<tuple<long,long,long,long,double,double,double,double,VarChar<1>,VarChar<1>,long,long,long,VarChar<25>,VarChar<10>,VarChar<44>,VarChar<1>,long,long,VarChar<1>,double,long,VarChar<15>,VarChar<15>,long,VarChar<79>,VarChar<1>,long,VarChar<25>,VarChar<40>,long,VarChar<15>,double,VarChar<10>,VarChar<117>,VarChar<1>>,bool>({});
tbb::enumerable_thread_specific<phmap::flat_hash_map<tuple<long,long,long,long,double,double,double,double,VarChar<1>,VarChar<1>,long,long,long,VarChar<25>,VarChar<10>,VarChar<44>,VarChar<1>,long,long,VarChar<1>,double,long,VarChar<15>,VarChar<15>,long,VarChar<79>,VarChar<1>,long,VarChar<25>,VarChar<40>,long,VarChar<15>,double,VarChar<10>,VarChar<117>,VarChar<1>>,bool>> v1435;
auto v1407 = phmap::flat_hash_map<tuple<VarChar<25>,long,long,long,double>,tuple<double>>({});
auto v1409 = phmap::flat_hash_map<tuple<VarChar<25>,long,long,long,double,double>,bool>({});	
	

auto v1410 = db->li_dataset_size; tbb::parallel_for(tbb::blocked_range<size_t>(0, v1410), [&](const tbb::blocked_range<size_t>& v1411){auto& v1415=v1414.local();for (size_t v1382=v1411.begin(), end=v1411.end(); v1382!=end; ++v1382){v1415[make_tuple(l_orderkey[v1382])] += make_tuple(l_quantity[v1382]);}});for (auto& local : v1414)AddMap<phmap::flat_hash_map<tuple<long>,tuple<double>>,tuple<long>,tuple<double>>(v1383, local);const auto& lineitem_0 = v1383;

for (auto& v1384 : lineitem_0){v1385[tuple_cat((v1384.first),move((v1384.second)))] += true;}const auto& lineitem_1 = v1385;

for (auto& v1386 : lineitem_1){if (( /* sum_quantity */get<1>((v1386.first)) > (long)300)){v1387[(v1386.first)] += (v1386.second);};}const auto& lineitem_2 = v1387;

for (auto& v1388 : lineitem_2){v1389[(v1388.first)] += (v1388.second);}const auto& lineitem_orders_isin_pre_ops = v1389;

for (auto& v1390 : lineitem_orders_isin_pre_ops){v1391[ /* l_orderkey */get<0>((v1390.first))] += true;}const auto& lineitem_orders_isin_build_index = v1391;

auto v1417 = db->ord_dataset_size; tbb::parallel_for(tbb::blocked_range<size_t>(0, v1417), [&](const tbb::blocked_range<size_t>& v1418){auto& v1422=v1421.local();for (size_t v1392=v1418.begin(), end=v1418.end(); v1392!=end; ++v1392){if (((lineitem_orders_isin_build_index).contains(o_orderkey[v1392]))){v1422[make_tuple(o_orderkey[v1392], o_custkey[v1392], o_orderstatus[v1392], o_totalprice[v1392], o_orderdate[v1392], o_orderpriority[v1392], o_clerk[v1392], o_shippriority[v1392], o_comment[v1392], o_NA[v1392])] += true;};}});for (auto& local : v1421)AddMap<phmap::flat_hash_map<tuple<long,long,VarChar<1>,double,long,VarChar<15>,VarChar<15>,long,VarChar<79>,VarChar<1>>,bool>,tuple<long,long,VarChar<1>,double,long,VarChar<15>,VarChar<15>,long,VarChar<79>,VarChar<1>>,bool>(v1393, local);const auto& customer_orders_probe_pre_ops = v1393;

auto v1424 = db->cu_dataset_size; tbb::parallel_for(tbb::blocked_range<size_t>(0, v1424), [&](const tbb::blocked_range<size_t>& v1425){auto& v1429=v1428.local();for (size_t v1394=v1425.begin(), end=v1425.end(); v1394!=end; ++v1394){v1429[c_custkey[v1394]] += phmap::flat_hash_map<tuple<long,VarChar<25>,VarChar<40>,long,VarChar<15>,double,VarChar<10>,VarChar<117>,VarChar<1>>,bool>({{make_tuple(c_custkey[v1394], c_name[v1394], c_address[v1394], c_nationkey[v1394], c_phone[v1394], c_acctbal[v1394], c_mktsegment[v1394], c_comment[v1394], c_NA[v1394]), true}});}});for (auto& local : v1428)AddMap<phmap::flat_hash_map<long,phmap::flat_hash_map<tuple<long,VarChar<25>,VarChar<40>,long,VarChar<15>,double,VarChar<10>,VarChar<117>,VarChar<1>>,bool>>,long,phmap::flat_hash_map<tuple<long,VarChar<25>,VarChar<40>,long,VarChar<15>,double,VarChar<10>,VarChar<117>,VarChar<1>>,bool>>(v1395, local);const auto& customer_orders_build_nest_dict = v1395;

for (auto& v1396 : customer_orders_probe_pre_ops){if (((customer_orders_build_nest_dict).contains( /* o_custkey */get<1>((v1396.first))))){for (auto& v1397 : (customer_orders_build_nest_dict).at( /* o_custkey */get<1>((v1396.first)))){v1399[tuple_cat((v1396.first),move((v1397.first)))] += true;};};}const auto& customer_orders_l1_build_pre_ops = v1399;

for (auto& v1400 : customer_orders_l1_build_pre_ops){v1401[ /* o_orderkey */get<0>((v1400.first))] += phmap::flat_hash_map<tuple<long,long,VarChar<1>,double,long,VarChar<15>,VarChar<15>,long,VarChar<79>,VarChar<1>,long,VarChar<25>,VarChar<40>,long,VarChar<15>,double,VarChar<10>,VarChar<117>,VarChar<1>>,bool>({{(v1400.first), (v1400.second)}});}const auto& customer_orders_l1_build_nest_dict = v1401;

auto v1431 = db->li_dataset_size; tbb::parallel_for(tbb::blocked_range<size_t>(0, v1431), [&](const tbb::blocked_range<size_t>& v1432){auto& v1436=v1435.local();for (size_t v1402=v1432.begin(), end=v1432.end(); v1402!=end; ++v1402){if (((customer_orders_l1_build_nest_dict).contains(l_orderkey[v1402]))){for (auto& v1403 : (customer_orders_l1_build_nest_dict).at(l_orderkey[v1402])){v1436[tuple_cat(make_tuple(l_orderkey[v1402], l_partkey[v1402], l_suppkey[v1402], l_linenumber[v1402], l_quantity[v1402], l_extendedprice[v1402], l_discount[v1402], l_tax[v1402], l_returnflag[v1402], l_linestatus[v1402], l_shipdate[v1402], l_commitdate[v1402], l_receiptdate[v1402], l_shipinstruct[v1402], l_shipmode[v1402], l_comment[v1402], l_NA[v1402]),move(make_tuple(o_orderkey[v1403], o_custkey[v1403], o_orderstatus[v1403], o_totalprice[v1403], o_orderdate[v1403], o_orderpriority[v1403], o_clerk[v1403], o_shippriority[v1403], o_comment[v1403], o_NA[v1403], c_custkey[v1403], c_name[v1403], c_address[v1403], c_nationkey[v1403], c_phone[v1403], c_acctbal[v1403], c_mktsegment[v1403], c_comment[v1403], c_NA[v1403])))] += true;};};}});for (auto& local : v1435)AddMap<phmap::flat_hash_map<tuple<long,long,long,long,double,double,double,double,VarChar<1>,VarChar<1>,long,long,long,VarChar<25>,VarChar<10>,VarChar<44>,VarChar<1>,long,long,VarChar<1>,double,long,VarChar<15>,VarChar<15>,long,VarChar<79>,VarChar<1>,long,VarChar<25>,VarChar<40>,long,VarChar<15>,double,VarChar<10>,VarChar<117>,VarChar<1>>,bool>,tuple<long,long,long,long,double,double,double,double,VarChar<1>,VarChar<1>,long,long,long,VarChar<25>,VarChar<10>,VarChar<44>,VarChar<1>,long,long,VarChar<1>,double,long,VarChar<15>,VarChar<15>,long,VarChar<79>,VarChar<1>,long,VarChar<25>,VarChar<40>,long,VarChar<15>,double,VarChar<10>,VarChar<117>,VarChar<1>>,bool>(v1405, local);const auto& customer_orders_l1_0 = v1405;

for (auto& v1406 : customer_orders_l1_0){v1407[make_tuple( /* c_name */get<28>((v1406.first)), /* c_custkey */get<27>((v1406.first)), /* o_orderkey */get<17>((v1406.first)), /* o_orderdate */get<21>((v1406.first)), /* o_totalprice */get<20>((v1406.first)))] += make_tuple( /* l_quantity */get<4>((v1406.first)));}const auto& customer_orders_l1_1 = v1407;

for (auto& v1408 : customer_orders_l1_1){v1409[tuple_cat((v1408.first),move((v1408.second)))] += true;}const auto& results = v1409;const auto& out = results;

    FastDict_s25iiiff_b* result = (FastDict_s25iiiff_b*)PyObject_CallObject(PyObject_CallObject(PyObject_GetAttrString(PyImport_Import(PyUnicode_FromString("test_unopt_fastdict_compiled")), (char*)"new_FastDict_s25iiiff_b"), nullptr), nullptr);
    *(result->dict) = out;

    return (PyObject*)PyObject_CallObject(PyObject_GetAttrString(PyImport_Import(PyUnicode_FromString("sdqlpy.fastd")), "fastd"), Py_BuildValue("(OO)", result, PyUnicode_FromString("test_unopt")));

}

static PyObject * q19(PyObject *self, PyObject* args)
{
	

    PyObject * db_;
    if (!PyArg_ParseTuple(args, "O", &db_)){
        PyErr_SetString(PyExc_ValueError,"Error while parsing the sum inputs.");
        return NULL;
    }
    
    
    DB dbobj;
    DB* db = &dbobj;
    
const static int numpy_initialized =  init_numpy();


	auto li_size = PyArray_Size(PyList_GetItem(PyList_GetItem(db_, 0), 0));
	db->li_dataset_size = li_size;
	auto l_orderkey = (long*)PyArray_DATA(PyList_GetItem(PyList_GetItem(db_, 0), 0));
	auto l_partkey = (long*)PyArray_DATA(PyList_GetItem(PyList_GetItem(db_, 0), 1));
	auto l_suppkey = (long*)PyArray_DATA(PyList_GetItem(PyList_GetItem(db_, 0), 2));
	auto l_linenumber = (long*)PyArray_DATA(PyList_GetItem(PyList_GetItem(db_, 0), 3));
	auto l_quantity = (double*)PyArray_DATA(PyList_GetItem(PyList_GetItem(db_, 0), 4));
	auto l_extendedprice = (double*)PyArray_DATA(PyList_GetItem(PyList_GetItem(db_, 0), 5));
	auto l_discount = (double*)PyArray_DATA(PyList_GetItem(PyList_GetItem(db_, 0), 6));
	auto l_tax = (double*)PyArray_DATA(PyList_GetItem(PyList_GetItem(db_, 0), 7));
	auto l_returnflag= (VarChar<1>*)PyArray_DATA(PyList_GetItem(PyList_GetItem(db_, 0), 8));
	auto l_linestatus= (VarChar<1>*)PyArray_DATA(PyList_GetItem(PyList_GetItem(db_, 0), 9));
	auto l_shipdate = (long*)PyArray_DATA(PyList_GetItem(PyList_GetItem(db_, 0), 10));
	auto l_commitdate = (long*)PyArray_DATA(PyList_GetItem(PyList_GetItem(db_, 0), 11));
	auto l_receiptdate = (long*)PyArray_DATA(PyList_GetItem(PyList_GetItem(db_, 0), 12));
	auto l_shipinstruct= (VarChar<25>*)PyArray_DATA(PyList_GetItem(PyList_GetItem(db_, 0), 13));
	auto l_shipmode= (VarChar<10>*)PyArray_DATA(PyList_GetItem(PyList_GetItem(db_, 0), 14));
	auto l_comment= (VarChar<44>*)PyArray_DATA(PyList_GetItem(PyList_GetItem(db_, 0), 15));
	auto l_NA= (VarChar<1>*)PyArray_DATA(PyList_GetItem(PyList_GetItem(db_, 0), 16));

	auto pa_size = PyArray_Size(PyList_GetItem(PyList_GetItem(db_, 1), 0));
	db->pa_dataset_size = pa_size;
	auto p_partkey = (long*)PyArray_DATA(PyList_GetItem(PyList_GetItem(db_, 1), 0));
	auto p_name= (VarChar<55>*)PyArray_DATA(PyList_GetItem(PyList_GetItem(db_, 1), 1));
	auto p_mfgr= (VarChar<25>*)PyArray_DATA(PyList_GetItem(PyList_GetItem(db_, 1), 2));
	auto p_brand= (VarChar<10>*)PyArray_DATA(PyList_GetItem(PyList_GetItem(db_, 1), 3));
	auto p_type= (VarChar<25>*)PyArray_DATA(PyList_GetItem(PyList_GetItem(db_, 1), 4));
	auto p_size = (long*)PyArray_DATA(PyList_GetItem(PyList_GetItem(db_, 1), 5));
	auto p_container= (VarChar<10>*)PyArray_DATA(PyList_GetItem(PyList_GetItem(db_, 1), 6));
	auto p_retailprice = (double*)PyArray_DATA(PyList_GetItem(PyList_GetItem(db_, 1), 7));
	auto p_comment= (VarChar<23>*)PyArray_DATA(PyList_GetItem(PyList_GetItem(db_, 1), 8));
	auto p_NA= (VarChar<1>*)PyArray_DATA(PyList_GetItem(PyList_GetItem(db_, 1), 9));



	
auto v1455 = phmap::flat_hash_map<tuple<long,VarChar<55>,VarChar<25>,VarChar<10>,VarChar<25>,long,VarChar<10>,double,VarChar<23>,VarChar<1>>,bool>({});
tbb::enumerable_thread_specific<phmap::flat_hash_map<tuple<long,VarChar<55>,VarChar<25>,VarChar<10>,VarChar<25>,long,VarChar<10>,double,VarChar<23>,VarChar<1>>,bool>> v1474;
auto v1457 = phmap::flat_hash_map<tuple<long,long,long,long,double,double,double,double,VarChar<1>,VarChar<1>,long,long,long,VarChar<25>,VarChar<10>,VarChar<44>,VarChar<1>>,bool>({});
tbb::enumerable_thread_specific<phmap::flat_hash_map<tuple<long,long,long,long,double,double,double,double,VarChar<1>,VarChar<1>,long,long,long,VarChar<25>,VarChar<10>,VarChar<44>,VarChar<1>>,bool>> v1481;
auto v1459 = phmap::flat_hash_map<long,phmap::flat_hash_map<tuple<long,VarChar<55>,VarChar<25>,VarChar<10>,VarChar<25>,long,VarChar<10>,double,VarChar<23>,VarChar<1>>,bool>>({});
auto v1463 = phmap::flat_hash_map<tuple<long,long,long,long,double,double,double,double,VarChar<1>,VarChar<1>,long,long,long,VarChar<25>,VarChar<10>,VarChar<44>,VarChar<1>,long,VarChar<55>,VarChar<25>,VarChar<10>,VarChar<25>,long,VarChar<10>,double,VarChar<23>,VarChar<1>>,bool>({});
auto v1465 = phmap::flat_hash_map<tuple<long,long,long,long,double,double,double,double,VarChar<1>,VarChar<1>,long,long,long,VarChar<25>,VarChar<10>,VarChar<44>,VarChar<1>,long,VarChar<55>,VarChar<25>,VarChar<10>,VarChar<25>,long,VarChar<10>,double,VarChar<23>,VarChar<1>>,bool>({});
auto v1467 = phmap::flat_hash_map<tuple<long,long,long,long,double,double,double,double,VarChar<1>,VarChar<1>,long,long,long,VarChar<25>,VarChar<10>,VarChar<44>,VarChar<1>,long,VarChar<55>,VarChar<25>,VarChar<10>,VarChar<25>,long,VarChar<10>,double,VarChar<23>,VarChar<1>,double>,bool>({});
auto v1469 = make_tuple(0.0);	
	const auto& brand12 = ConstantString("Brand#12", 9);const auto& smcase = ConstantString("SM CASE", 8);const auto& smbox = ConstantString("SM BOX", 7);const auto& smpack = ConstantString("SM PACK", 8);const auto& smpkg = ConstantString("SM PKG", 7);const auto& brand23 = ConstantString("Brand#23", 9);const auto& medbag = ConstantString("MED BAG", 8);const auto& medbox = ConstantString("MED BOX", 8);const auto& medpkg = ConstantString("MED PKG", 8);const auto& medpack = ConstantString("MED PACK", 9);const auto& brand34 = ConstantString("Brand#34", 9);const auto& lgcase = ConstantString("LG CASE", 8);const auto& lgbox = ConstantString("LG BOX", 7);const auto& lgpack = ConstantString("LG PACK", 8);const auto& lgpkg = ConstantString("LG PKG", 7);const auto& air = ConstantString("AIR", 4);const auto& airreg = ConstantString("AIR REG", 8);const auto& deliverinperson = ConstantString("DELIVER IN PERSON", 18);

auto v1470 = db->pa_dataset_size; tbb::parallel_for(tbb::blocked_range<size_t>(0, v1470), [&](const tbb::blocked_range<size_t>& v1471){auto& v1475=v1474.local();for (size_t v1454=v1471.begin(), end=v1471.end(); v1454!=end; ++v1454){if (((((((p_brand[v1454] == brand12) && ((((p_container[v1454] == smpkg) || (p_container[v1454] == smpack)) || (p_container[v1454] == smcase)) || (p_container[v1454] == smbox))) && (p_size[v1454] >= (long)1)) && (p_size[v1454] <= (long)5)) || ((((p_brand[v1454] == brand23) && ((((p_container[v1454] == medpack) || (p_container[v1454] == medpkg)) || (p_container[v1454] == medbag)) || (p_container[v1454] == medbox))) && (p_size[v1454] >= (long)1)) && (p_size[v1454] <= (long)10))) || ((((p_brand[v1454] == brand34) && ((((p_container[v1454] == lgpkg) || (p_container[v1454] == lgpack)) || (p_container[v1454] == lgcase)) || (p_container[v1454] == lgbox))) && (p_size[v1454] >= (long)1)) && (p_size[v1454] <= (long)15)))){v1475[make_tuple(p_partkey[v1454], p_name[v1454], p_mfgr[v1454], p_brand[v1454], p_type[v1454], p_size[v1454], p_container[v1454], p_retailprice[v1454], p_comment[v1454], p_NA[v1454])] += true;};}});for (auto& local : v1474)AddMap<phmap::flat_hash_map<tuple<long,VarChar<55>,VarChar<25>,VarChar<10>,VarChar<25>,long,VarChar<10>,double,VarChar<23>,VarChar<1>>,bool>,tuple<long,VarChar<55>,VarChar<25>,VarChar<10>,VarChar<25>,long,VarChar<10>,double,VarChar<23>,VarChar<1>>,bool>(v1455, local);const auto& part_lineitem_build_pre_ops = v1455;

auto v1477 = db->li_dataset_size; tbb::parallel_for(tbb::blocked_range<size_t>(0, v1477), [&](const tbb::blocked_range<size_t>& v1478){auto& v1482=v1481.local();for (size_t v1456=v1478.begin(), end=v1478.end(); v1456!=end; ++v1456){if ((((l_shipmode[v1456] == air) || (l_shipmode[v1456] == airreg)) && (l_shipinstruct[v1456] == deliverinperson))){v1482[make_tuple(l_orderkey[v1456], l_partkey[v1456], l_suppkey[v1456], l_linenumber[v1456], l_quantity[v1456], l_extendedprice[v1456], l_discount[v1456], l_tax[v1456], l_returnflag[v1456], l_linestatus[v1456], l_shipdate[v1456], l_commitdate[v1456], l_receiptdate[v1456], l_shipinstruct[v1456], l_shipmode[v1456], l_comment[v1456], l_NA[v1456])] += true;};}});for (auto& local : v1481)AddMap<phmap::flat_hash_map<tuple<long,long,long,long,double,double,double,double,VarChar<1>,VarChar<1>,long,long,long,VarChar<25>,VarChar<10>,VarChar<44>,VarChar<1>>,bool>,tuple<long,long,long,long,double,double,double,double,VarChar<1>,VarChar<1>,long,long,long,VarChar<25>,VarChar<10>,VarChar<44>,VarChar<1>>,bool>(v1457, local);const auto& part_lineitem_probe_pre_ops = v1457;

for (auto& v1458 : part_lineitem_build_pre_ops){v1459[ /* p_partkey */get<0>((v1458.first))] += phmap::flat_hash_map<tuple<long,VarChar<55>,VarChar<25>,VarChar<10>,VarChar<25>,long,VarChar<10>,double,VarChar<23>,VarChar<1>>,bool>({{(v1458.first), (v1458.second)}});}const auto& part_lineitem_build_nest_dict = v1459;

for (auto& v1460 : part_lineitem_probe_pre_ops){if (((part_lineitem_build_nest_dict).contains( /* l_partkey */get<1>((v1460.first))))){for (auto& v1461 : (part_lineitem_build_nest_dict).at( /* l_partkey */get<1>((v1460.first)))){v1463[tuple_cat((v1460.first),move((v1461.first)))] += true;};};}const auto& part_lineitem_0 = v1463;

for (auto& v1464 : part_lineitem_0){if ((((( /* p_brand */get<20>((v1464.first)) == brand12) && (( /* l_quantity */get<4>((v1464.first)) >= (long)1) && ( /* l_quantity */get<4>((v1464.first)) <= (long)11))) || (( /* p_brand */get<20>((v1464.first)) == brand23) && (( /* l_quantity */get<4>((v1464.first)) >= (long)10) && ( /* l_quantity */get<4>((v1464.first)) <= (long)20)))) || (( /* p_brand */get<20>((v1464.first)) == brand34) && (( /* l_quantity */get<4>((v1464.first)) >= (long)20) && ( /* l_quantity */get<4>((v1464.first)) <= (long)30))))){v1465[(v1464.first)] += (v1464.second);};}const auto& part_lineitem_1 = v1465;

for (auto& v1466 : part_lineitem_1){v1467[tuple_cat((v1466.first),move(make_tuple(( /* l_extendedprice */get<5>((v1466.first)) * (1.0 -  /* l_discount */get<6>((v1466.first)))))))] += (v1466.second);}const auto& part_lineitem_2 = v1467;

for (auto& v1468 : part_lineitem_2){make_tuple( /* revenue */get<27>((v1468.first)));}const auto& part_lineitem_3 = v1469;const auto& results = phmap::flat_hash_map<tuple<double>,bool>({{part_lineitem_3, true}});const auto& out = results;

    FastDict_f_b* result = (FastDict_f_b*)PyObject_CallObject(PyObject_CallObject(PyObject_GetAttrString(PyImport_Import(PyUnicode_FromString("test_unopt_fastdict_compiled")), (char*)"new_FastDict_f_b"), nullptr), nullptr);
    *(result->dict) = out;

    return (PyObject*)PyObject_CallObject(PyObject_GetAttrString(PyImport_Import(PyUnicode_FromString("sdqlpy.fastd")), "fastd"), Py_BuildValue("(OO)", result, PyUnicode_FromString("test_unopt")));

}

static PyObject * q20(PyObject *self, PyObject* args)
{
	

    PyObject * db_;
    if (!PyArg_ParseTuple(args, "O", &db_)){
        PyErr_SetString(PyExc_ValueError,"Error while parsing the sum inputs.");
        return NULL;
    }
    
    
    DB dbobj;
    DB* db = &dbobj;
    
const static int numpy_initialized =  init_numpy();


	auto su_size = PyArray_Size(PyList_GetItem(PyList_GetItem(db_, 0), 0));
	db->su_dataset_size = su_size;
	auto s_suppkey = (long*)PyArray_DATA(PyList_GetItem(PyList_GetItem(db_, 0), 0));
	auto s_name= (VarChar<25>*)PyArray_DATA(PyList_GetItem(PyList_GetItem(db_, 0), 1));
	auto s_address= (VarChar<40>*)PyArray_DATA(PyList_GetItem(PyList_GetItem(db_, 0), 2));
	auto s_nationkey = (long*)PyArray_DATA(PyList_GetItem(PyList_GetItem(db_, 0), 3));
	auto s_phone= (VarChar<15>*)PyArray_DATA(PyList_GetItem(PyList_GetItem(db_, 0), 4));
	auto s_acctbal = (double*)PyArray_DATA(PyList_GetItem(PyList_GetItem(db_, 0), 5));
	auto s_comment= (VarChar<101>*)PyArray_DATA(PyList_GetItem(PyList_GetItem(db_, 0), 6));
	auto s_NA= (VarChar<1>*)PyArray_DATA(PyList_GetItem(PyList_GetItem(db_, 0), 7));

	auto na_size = PyArray_Size(PyList_GetItem(PyList_GetItem(db_, 1), 0));
	db->na_dataset_size = na_size;
	auto n_nationkey = (long*)PyArray_DATA(PyList_GetItem(PyList_GetItem(db_, 1), 0));
	auto n_name= (VarChar<25>*)PyArray_DATA(PyList_GetItem(PyList_GetItem(db_, 1), 1));
	auto n_regionkey = (long*)PyArray_DATA(PyList_GetItem(PyList_GetItem(db_, 1), 2));
	auto n_comment= (VarChar<152>*)PyArray_DATA(PyList_GetItem(PyList_GetItem(db_, 1), 3));
	auto n_NA= (VarChar<1>*)PyArray_DATA(PyList_GetItem(PyList_GetItem(db_, 1), 4));

	auto ps_size = PyArray_Size(PyList_GetItem(PyList_GetItem(db_, 2), 0));
	db->ps_dataset_size = ps_size;
	auto ps_partkey = (long*)PyArray_DATA(PyList_GetItem(PyList_GetItem(db_, 2), 0));
	auto ps_suppkey = (long*)PyArray_DATA(PyList_GetItem(PyList_GetItem(db_, 2), 1));
	auto ps_availqty = (double*)PyArray_DATA(PyList_GetItem(PyList_GetItem(db_, 2), 2));
	auto ps_supplycost = (double*)PyArray_DATA(PyList_GetItem(PyList_GetItem(db_, 2), 3));
	auto ps_comment= (VarChar<199>*)PyArray_DATA(PyList_GetItem(PyList_GetItem(db_, 2), 4));
	auto ps_NA= (VarChar<1>*)PyArray_DATA(PyList_GetItem(PyList_GetItem(db_, 2), 5));

	auto pa_size = PyArray_Size(PyList_GetItem(PyList_GetItem(db_, 3), 0));
	db->pa_dataset_size = pa_size;
	auto p_partkey = (long*)PyArray_DATA(PyList_GetItem(PyList_GetItem(db_, 3), 0));
	auto p_name= (VarChar<55>*)PyArray_DATA(PyList_GetItem(PyList_GetItem(db_, 3), 1));
	auto p_mfgr= (VarChar<25>*)PyArray_DATA(PyList_GetItem(PyList_GetItem(db_, 3), 2));
	auto p_brand= (VarChar<10>*)PyArray_DATA(PyList_GetItem(PyList_GetItem(db_, 3), 3));
	auto p_type= (VarChar<25>*)PyArray_DATA(PyList_GetItem(PyList_GetItem(db_, 3), 4));
	auto p_size = (long*)PyArray_DATA(PyList_GetItem(PyList_GetItem(db_, 3), 5));
	auto p_container= (VarChar<10>*)PyArray_DATA(PyList_GetItem(PyList_GetItem(db_, 3), 6));
	auto p_retailprice = (double*)PyArray_DATA(PyList_GetItem(PyList_GetItem(db_, 3), 7));
	auto p_comment= (VarChar<23>*)PyArray_DATA(PyList_GetItem(PyList_GetItem(db_, 3), 8));
	auto p_NA= (VarChar<1>*)PyArray_DATA(PyList_GetItem(PyList_GetItem(db_, 3), 9));

	auto li_size = PyArray_Size(PyList_GetItem(PyList_GetItem(db_, 4), 0));
	db->li_dataset_size = li_size;
	auto l_orderkey = (long*)PyArray_DATA(PyList_GetItem(PyList_GetItem(db_, 4), 0));
	auto l_partkey = (long*)PyArray_DATA(PyList_GetItem(PyList_GetItem(db_, 4), 1));
	auto l_suppkey = (long*)PyArray_DATA(PyList_GetItem(PyList_GetItem(db_, 4), 2));
	auto l_linenumber = (long*)PyArray_DATA(PyList_GetItem(PyList_GetItem(db_, 4), 3));
	auto l_quantity = (double*)PyArray_DATA(PyList_GetItem(PyList_GetItem(db_, 4), 4));
	auto l_extendedprice = (double*)PyArray_DATA(PyList_GetItem(PyList_GetItem(db_, 4), 5));
	auto l_discount = (double*)PyArray_DATA(PyList_GetItem(PyList_GetItem(db_, 4), 6));
	auto l_tax = (double*)PyArray_DATA(PyList_GetItem(PyList_GetItem(db_, 4), 7));
	auto l_returnflag= (VarChar<1>*)PyArray_DATA(PyList_GetItem(PyList_GetItem(db_, 4), 8));
	auto l_linestatus= (VarChar<1>*)PyArray_DATA(PyList_GetItem(PyList_GetItem(db_, 4), 9));
	auto l_shipdate = (long*)PyArray_DATA(PyList_GetItem(PyList_GetItem(db_, 4), 10));
	auto l_commitdate = (long*)PyArray_DATA(PyList_GetItem(PyList_GetItem(db_, 4), 11));
	auto l_receiptdate = (long*)PyArray_DATA(PyList_GetItem(PyList_GetItem(db_, 4), 12));
	auto l_shipinstruct= (VarChar<25>*)PyArray_DATA(PyList_GetItem(PyList_GetItem(db_, 4), 13));
	auto l_shipmode= (VarChar<10>*)PyArray_DATA(PyList_GetItem(PyList_GetItem(db_, 4), 14));
	auto l_comment= (VarChar<44>*)PyArray_DATA(PyList_GetItem(PyList_GetItem(db_, 4), 15));
	auto l_NA= (VarChar<1>*)PyArray_DATA(PyList_GetItem(PyList_GetItem(db_, 4), 16));



	
auto v1525 = phmap::flat_hash_map<tuple<long,VarChar<25>,long,VarChar<152>,VarChar<1>>,bool>({});
tbb::enumerable_thread_specific<phmap::flat_hash_map<tuple<long,VarChar<25>,long,VarChar<152>,VarChar<1>>,bool>> v1568;
auto v1527 = phmap::flat_hash_map<tuple<long,long,long,long,double,double,double,double,VarChar<1>,VarChar<1>,long,long,long,VarChar<25>,VarChar<10>,VarChar<44>,VarChar<1>>,bool>({});
tbb::enumerable_thread_specific<phmap::flat_hash_map<tuple<long,long,long,long,double,double,double,double,VarChar<1>,VarChar<1>,long,long,long,VarChar<25>,VarChar<10>,VarChar<44>,VarChar<1>>,bool>> v1575;
auto v1529 = phmap::flat_hash_map<tuple<long,VarChar<55>,VarChar<25>,VarChar<10>,VarChar<25>,long,VarChar<10>,double,VarChar<23>,VarChar<1>>,bool>({});
tbb::enumerable_thread_specific<phmap::flat_hash_map<tuple<long,VarChar<55>,VarChar<25>,VarChar<10>,VarChar<25>,long,VarChar<10>,double,VarChar<23>,VarChar<1>>,bool>> v1582;
auto v1531 = phmap::flat_hash_map<long,bool>({});
auto v1533 = phmap::flat_hash_map<tuple<long,long,long,long,double,double,double,double,VarChar<1>,VarChar<1>,long,long,long,VarChar<25>,VarChar<10>,VarChar<44>,VarChar<1>>,bool>({});
auto v1535 = phmap::flat_hash_map<long,bool>({});
tbb::enumerable_thread_specific<phmap::flat_hash_map<long,bool>> v1589;
auto v1537 = phmap::flat_hash_map<tuple<long,long,long,long,double,double,double,double,VarChar<1>,VarChar<1>,long,long,long,VarChar<25>,VarChar<10>,VarChar<44>,VarChar<1>>,bool>({});
auto v1539 = phmap::flat_hash_map<tuple<long,long>,tuple<double>>({});
auto v1541 = phmap::flat_hash_map<tuple<long,long,double>,bool>({});
auto v1543 = phmap::flat_hash_map<tuple<long,long>,phmap::flat_hash_map<tuple<long,long,double>,bool>>({});
auto v1547 = phmap::flat_hash_map<tuple<long,long,double,double,VarChar<199>,VarChar<1>,long,long,double>,bool>({});
tbb::enumerable_thread_specific<phmap::flat_hash_map<tuple<long,long,double,double,VarChar<199>,VarChar<1>,long,long,double>,bool>> v1596;
auto v1549 = phmap::flat_hash_map<tuple<long,long,double,double,VarChar<199>,VarChar<1>,long,long,double>,bool>({});
auto v1551 = phmap::flat_hash_map<long,bool>({});
auto v1553 = phmap::flat_hash_map<tuple<long,VarChar<25>,VarChar<40>,long,VarChar<15>,double,VarChar<101>,VarChar<1>>,bool>({});
tbb::enumerable_thread_specific<phmap::flat_hash_map<tuple<long,VarChar<25>,VarChar<40>,long,VarChar<15>,double,VarChar<101>,VarChar<1>>,bool>> v1603;
auto v1555 = phmap::flat_hash_map<long,phmap::flat_hash_map<tuple<long,VarChar<25>,long,VarChar<152>,VarChar<1>>,bool>>({});
auto v1559 = phmap::flat_hash_map<tuple<long,VarChar<25>,VarChar<40>,long,VarChar<15>,double,VarChar<101>,VarChar<1>,long,VarChar<25>,long,VarChar<152>,VarChar<1>>,bool>({});
auto v1561 = phmap::flat_hash_map<tuple<long,VarChar<25>,VarChar<40>,long,VarChar<15>,double,VarChar<101>,VarChar<1>,long,VarChar<25>,long,VarChar<152>,VarChar<1>>,phmap::flat_hash_map<tuple<VarChar<25>,VarChar<40>>,bool>>({});
auto v1563 = phmap::flat_hash_map<tuple<VarChar<25>,VarChar<40>>,bool>({});	
	const auto& canada = ConstantString("CANADA", 7);const auto& forest = ConstantString("forest", 7);

auto v1564 = db->na_dataset_size; tbb::parallel_for(tbb::blocked_range<size_t>(0, v1564), [&](const tbb::blocked_range<size_t>& v1565){auto& v1569=v1568.local();for (size_t v1524=v1565.begin(), end=v1565.end(); v1524!=end; ++v1524){if ((n_name[v1524] == canada)){v1569[make_tuple(n_nationkey[v1524], n_name[v1524], n_regionkey[v1524], n_comment[v1524], n_NA[v1524])] += true;};}});for (auto& local : v1568)AddMap<phmap::flat_hash_map<tuple<long,VarChar<25>,long,VarChar<152>,VarChar<1>>,bool>,tuple<long,VarChar<25>,long,VarChar<152>,VarChar<1>>,bool>(v1525, local);const auto& nation_supplier_build_pre_ops = v1525;

auto v1571 = db->li_dataset_size; tbb::parallel_for(tbb::blocked_range<size_t>(0, v1571), [&](const tbb::blocked_range<size_t>& v1572){auto& v1576=v1575.local();for (size_t v1526=v1572.begin(), end=v1572.end(); v1526!=end; ++v1526){if (((l_shipdate[v1526] >= (long)19940101) && (l_shipdate[v1526] < (long)19950101))){v1576[make_tuple(l_orderkey[v1526], l_partkey[v1526], l_suppkey[v1526], l_linenumber[v1526], l_quantity[v1526], l_extendedprice[v1526], l_discount[v1526], l_tax[v1526], l_returnflag[v1526], l_linestatus[v1526], l_shipdate[v1526], l_commitdate[v1526], l_receiptdate[v1526], l_shipinstruct[v1526], l_shipmode[v1526], l_comment[v1526], l_NA[v1526])] += true;};}});for (auto& local : v1575)AddMap<phmap::flat_hash_map<tuple<long,long,long,long,double,double,double,double,VarChar<1>,VarChar<1>,long,long,long,VarChar<25>,VarChar<10>,VarChar<44>,VarChar<1>>,bool>,tuple<long,long,long,long,double,double,double,double,VarChar<1>,VarChar<1>,long,long,long,VarChar<25>,VarChar<10>,VarChar<44>,VarChar<1>>,bool>(v1527, local);const auto& lineitem_0 = v1527;

auto v1578 = db->pa_dataset_size; tbb::parallel_for(tbb::blocked_range<size_t>(0, v1578), [&](const tbb::blocked_range<size_t>& v1579){auto& v1583=v1582.local();for (size_t v1528=v1579.begin(), end=v1579.end(); v1528!=end; ++v1528){if ((p_name[v1528].startsWith(forest))){v1583[make_tuple(p_partkey[v1528], p_name[v1528], p_mfgr[v1528], p_brand[v1528], p_type[v1528], p_size[v1528], p_container[v1528], p_retailprice[v1528], p_comment[v1528], p_NA[v1528])] += true;};}});for (auto& local : v1582)AddMap<phmap::flat_hash_map<tuple<long,VarChar<55>,VarChar<25>,VarChar<10>,VarChar<25>,long,VarChar<10>,double,VarChar<23>,VarChar<1>>,bool>,tuple<long,VarChar<55>,VarChar<25>,VarChar<10>,VarChar<25>,long,VarChar<10>,double,VarChar<23>,VarChar<1>>,bool>(v1529, local);const auto& part_lineitem_isin_pre_ops = v1529;

for (auto& v1530 : part_lineitem_isin_pre_ops){v1531[ /* p_partkey */get<0>((v1530.first))] += true;}const auto& part_lineitem_isin_build_index = v1531;

for (auto& v1532 : lineitem_0){if (((part_lineitem_isin_build_index).contains( /* l_partkey */get<1>((v1532.first))))){v1533[(v1532.first)] += (v1532.second);};}const auto& lineitem_1 = v1533;

auto v1585 = db->su_dataset_size; tbb::parallel_for(tbb::blocked_range<size_t>(0, v1585), [&](const tbb::blocked_range<size_t>& v1586){auto& v1590=v1589.local();for (size_t v1534=v1586.begin(), end=v1586.end(); v1534!=end; ++v1534){v1590[s_suppkey[v1534]] += true;}});for (auto& local : v1589)AddMap<phmap::flat_hash_map<long,bool>,long,bool>(v1535, local);const auto& supplier_lineitem_isin_build_index = v1535;

for (auto& v1536 : lineitem_1){if (((supplier_lineitem_isin_build_index).contains( /* l_suppkey */get<2>((v1536.first))))){v1537[(v1536.first)] += (v1536.second);};}const auto& lineitem_2 = v1537;

for (auto& v1538 : lineitem_2){v1539[make_tuple( /* l_partkey */get<1>((v1538.first)), /* l_suppkey */get<2>((v1538.first)))] += make_tuple( /* l_quantity */get<4>((v1538.first)));}const auto& lineitem_3 = v1539;

for (auto& v1540 : lineitem_3){v1541[tuple_cat((v1540.first),move((v1540.second)))] += true;}const auto& lineitem_partsupp_build_pre_ops = v1541;

for (auto& v1542 : lineitem_partsupp_build_pre_ops){v1543[make_tuple( /* l_partkey */get<0>((v1542.first)), /* l_suppkey */get<1>((v1542.first)))] += phmap::flat_hash_map<tuple<long,long,double>,bool>({{(v1542.first), (v1542.second)}});}const auto& lineitem_partsupp_build_nest_dict = v1543;

auto v1592 = db->ps_dataset_size; tbb::parallel_for(tbb::blocked_range<size_t>(0, v1592), [&](const tbb::blocked_range<size_t>& v1593){auto& v1597=v1596.local();for (size_t v1544=v1593.begin(), end=v1593.end(); v1544!=end; ++v1544){if (((lineitem_partsupp_build_nest_dict).contains(make_tuple(ps_partkey[v1544],ps_suppkey[v1544])))){for (auto& v1545 : (lineitem_partsupp_build_nest_dict).at(make_tuple(ps_partkey[v1544],ps_suppkey[v1544]))){v1597[tuple_cat(make_tuple(ps_partkey[v1544], ps_suppkey[v1544], ps_availqty[v1544], ps_supplycost[v1544], ps_comment[v1544], ps_NA[v1544]),move(make_tuple(l_partkey[v1545], l_suppkey[v1545], sum_quantity[v1545])))] += true;};};}});for (auto& local : v1596)AddMap<phmap::flat_hash_map<tuple<long,long,double,double,VarChar<199>,VarChar<1>,long,long,double>,bool>,tuple<long,long,double,double,VarChar<199>,VarChar<1>,long,long,double>,bool>(v1547, local);const auto& lineitem_partsupp_0 = v1547;

for (auto& v1548 : lineitem_partsupp_0){if (( /* ps_availqty */get<2>((v1548.first)) > ( /* sum_quantity */get<8>((v1548.first)) * 0.5))){v1549[(v1548.first)] += (v1548.second);};}const auto& lineitem_partsupp_supplier_isin_pre_ops = v1549;

for (auto& v1550 : lineitem_partsupp_supplier_isin_pre_ops){v1551[ /* l_suppkey */get<7>((v1550.first))] += true;}const auto& lineitem_partsupp_supplier_isin_build_index = v1551;

auto v1599 = db->su_dataset_size; tbb::parallel_for(tbb::blocked_range<size_t>(0, v1599), [&](const tbb::blocked_range<size_t>& v1600){auto& v1604=v1603.local();for (size_t v1552=v1600.begin(), end=v1600.end(); v1552!=end; ++v1552){if (((lineitem_partsupp_supplier_isin_build_index).contains(s_suppkey[v1552]))){v1604[make_tuple(s_suppkey[v1552], s_name[v1552], s_address[v1552], s_nationkey[v1552], s_phone[v1552], s_acctbal[v1552], s_comment[v1552], s_NA[v1552])] += true;};}});for (auto& local : v1603)AddMap<phmap::flat_hash_map<tuple<long,VarChar<25>,VarChar<40>,long,VarChar<15>,double,VarChar<101>,VarChar<1>>,bool>,tuple<long,VarChar<25>,VarChar<40>,long,VarChar<15>,double,VarChar<101>,VarChar<1>>,bool>(v1553, local);const auto& nation_supplier_probe_pre_ops = v1553;

for (auto& v1554 : nation_supplier_build_pre_ops){v1555[ /* n_nationkey */get<0>((v1554.first))] += phmap::flat_hash_map<tuple<long,VarChar<25>,long,VarChar<152>,VarChar<1>>,bool>({{(v1554.first), (v1554.second)}});}const auto& nation_supplier_build_nest_dict = v1555;

for (auto& v1556 : nation_supplier_probe_pre_ops){if (((nation_supplier_build_nest_dict).contains( /* s_nationkey */get<3>((v1556.first))))){for (auto& v1557 : (nation_supplier_build_nest_dict).at( /* s_nationkey */get<3>((v1556.first)))){v1559[tuple_cat((v1556.first),move((v1557.first)))] += true;};};}const auto& nation_supplier_0 = v1559;

for (auto& v1560 : nation_supplier_0){v1561[(v1560.first)] += phmap::flat_hash_map<tuple<VarChar<25>,VarChar<40>>,bool>({{make_tuple( /* s_name */get<1>((v1560.first)), /* s_address */get<2>((v1560.first))), true}});}const auto& nation_supplier_1 = v1561;

for (auto& v1562 : nation_supplier_1){(v1562.second);}const auto& results = v1563;const auto& out = results;

    FastDict_s25s40_b* result = (FastDict_s25s40_b*)PyObject_CallObject(PyObject_CallObject(PyObject_GetAttrString(PyImport_Import(PyUnicode_FromString("test_unopt_fastdict_compiled")), (char*)"new_FastDict_s25s40_b"), nullptr), nullptr);
    *(result->dict) = out;

    return (PyObject*)PyObject_CallObject(PyObject_GetAttrString(PyImport_Import(PyUnicode_FromString("sdqlpy.fastd")), "fastd"), Py_BuildValue("(OO)", result, PyUnicode_FromString("test_unopt")));

}

static PyObject * q21(PyObject *self, PyObject* args)
{
	

    PyObject * db_;
    if (!PyArg_ParseTuple(args, "O", &db_)){
        PyErr_SetString(PyExc_ValueError,"Error while parsing the sum inputs.");
        return NULL;
    }
    
    
    DB dbobj;
    DB* db = &dbobj;
    
const static int numpy_initialized =  init_numpy();


	auto su_size = PyArray_Size(PyList_GetItem(PyList_GetItem(db_, 0), 0));
	db->su_dataset_size = su_size;
	auto s_suppkey = (long*)PyArray_DATA(PyList_GetItem(PyList_GetItem(db_, 0), 0));
	auto s_name= (VarChar<25>*)PyArray_DATA(PyList_GetItem(PyList_GetItem(db_, 0), 1));
	auto s_address= (VarChar<40>*)PyArray_DATA(PyList_GetItem(PyList_GetItem(db_, 0), 2));
	auto s_nationkey = (long*)PyArray_DATA(PyList_GetItem(PyList_GetItem(db_, 0), 3));
	auto s_phone= (VarChar<15>*)PyArray_DATA(PyList_GetItem(PyList_GetItem(db_, 0), 4));
	auto s_acctbal = (double*)PyArray_DATA(PyList_GetItem(PyList_GetItem(db_, 0), 5));
	auto s_comment= (VarChar<101>*)PyArray_DATA(PyList_GetItem(PyList_GetItem(db_, 0), 6));
	auto s_NA= (VarChar<1>*)PyArray_DATA(PyList_GetItem(PyList_GetItem(db_, 0), 7));

	auto li_size = PyArray_Size(PyList_GetItem(PyList_GetItem(db_, 1), 0));
	db->li_dataset_size = li_size;
	auto l_orderkey = (long*)PyArray_DATA(PyList_GetItem(PyList_GetItem(db_, 1), 0));
	auto l_partkey = (long*)PyArray_DATA(PyList_GetItem(PyList_GetItem(db_, 1), 1));
	auto l_suppkey = (long*)PyArray_DATA(PyList_GetItem(PyList_GetItem(db_, 1), 2));
	auto l_linenumber = (long*)PyArray_DATA(PyList_GetItem(PyList_GetItem(db_, 1), 3));
	auto l_quantity = (double*)PyArray_DATA(PyList_GetItem(PyList_GetItem(db_, 1), 4));
	auto l_extendedprice = (double*)PyArray_DATA(PyList_GetItem(PyList_GetItem(db_, 1), 5));
	auto l_discount = (double*)PyArray_DATA(PyList_GetItem(PyList_GetItem(db_, 1), 6));
	auto l_tax = (double*)PyArray_DATA(PyList_GetItem(PyList_GetItem(db_, 1), 7));
	auto l_returnflag= (VarChar<1>*)PyArray_DATA(PyList_GetItem(PyList_GetItem(db_, 1), 8));
	auto l_linestatus= (VarChar<1>*)PyArray_DATA(PyList_GetItem(PyList_GetItem(db_, 1), 9));
	auto l_shipdate = (long*)PyArray_DATA(PyList_GetItem(PyList_GetItem(db_, 1), 10));
	auto l_commitdate = (long*)PyArray_DATA(PyList_GetItem(PyList_GetItem(db_, 1), 11));
	auto l_receiptdate = (long*)PyArray_DATA(PyList_GetItem(PyList_GetItem(db_, 1), 12));
	auto l_shipinstruct= (VarChar<25>*)PyArray_DATA(PyList_GetItem(PyList_GetItem(db_, 1), 13));
	auto l_shipmode= (VarChar<10>*)PyArray_DATA(PyList_GetItem(PyList_GetItem(db_, 1), 14));
	auto l_comment= (VarChar<44>*)PyArray_DATA(PyList_GetItem(PyList_GetItem(db_, 1), 15));
	auto l_NA= (VarChar<1>*)PyArray_DATA(PyList_GetItem(PyList_GetItem(db_, 1), 16));

	auto ord_size = PyArray_Size(PyList_GetItem(PyList_GetItem(db_, 2), 0));
	db->ord_dataset_size = ord_size;
	auto o_orderkey = (long*)PyArray_DATA(PyList_GetItem(PyList_GetItem(db_, 2), 0));
	auto o_custkey = (long*)PyArray_DATA(PyList_GetItem(PyList_GetItem(db_, 2), 1));
	auto o_orderstatus= (VarChar<1>*)PyArray_DATA(PyList_GetItem(PyList_GetItem(db_, 2), 2));
	auto o_totalprice = (double*)PyArray_DATA(PyList_GetItem(PyList_GetItem(db_, 2), 3));
	auto o_orderdate = (long*)PyArray_DATA(PyList_GetItem(PyList_GetItem(db_, 2), 4));
	auto o_orderpriority= (VarChar<15>*)PyArray_DATA(PyList_GetItem(PyList_GetItem(db_, 2), 5));
	auto o_clerk= (VarChar<15>*)PyArray_DATA(PyList_GetItem(PyList_GetItem(db_, 2), 6));
	auto o_shippriority = (long*)PyArray_DATA(PyList_GetItem(PyList_GetItem(db_, 2), 7));
	auto o_comment= (VarChar<79>*)PyArray_DATA(PyList_GetItem(PyList_GetItem(db_, 2), 8));
	auto o_NA= (VarChar<1>*)PyArray_DATA(PyList_GetItem(PyList_GetItem(db_, 2), 9));

	auto na_size = PyArray_Size(PyList_GetItem(PyList_GetItem(db_, 3), 0));
	db->na_dataset_size = na_size;
	auto n_nationkey = (long*)PyArray_DATA(PyList_GetItem(PyList_GetItem(db_, 3), 0));
	auto n_name= (VarChar<25>*)PyArray_DATA(PyList_GetItem(PyList_GetItem(db_, 3), 1));
	auto n_regionkey = (long*)PyArray_DATA(PyList_GetItem(PyList_GetItem(db_, 3), 2));
	auto n_comment= (VarChar<152>*)PyArray_DATA(PyList_GetItem(PyList_GetItem(db_, 3), 3));
	auto n_NA= (VarChar<1>*)PyArray_DATA(PyList_GetItem(PyList_GetItem(db_, 3), 4));



	
auto v1659 = phmap::flat_hash_map<tuple<long,long,VarChar<1>,double,long,VarChar<15>,VarChar<15>,long,VarChar<79>,VarChar<1>>,bool>({});
tbb::enumerable_thread_specific<phmap::flat_hash_map<tuple<long,long,VarChar<1>,double,long,VarChar<15>,VarChar<15>,long,VarChar<79>,VarChar<1>>,bool>> v1714;
auto v1661 = phmap::flat_hash_map<tuple<long,VarChar<25>,long,VarChar<152>,VarChar<1>>,bool>({});
tbb::enumerable_thread_specific<phmap::flat_hash_map<tuple<long,VarChar<25>,long,VarChar<152>,VarChar<1>>,bool>> v1721;
auto v1663 = phmap::flat_hash_map<long,phmap::flat_hash_map<tuple<long,VarChar<25>,long,VarChar<152>,VarChar<1>>,bool>>({});
auto v1667 = phmap::flat_hash_map<tuple<long,VarChar<25>,VarChar<40>,long,VarChar<15>,double,VarChar<101>,VarChar<1>,long,VarChar<25>,long,VarChar<152>,VarChar<1>>,bool>({});
tbb::enumerable_thread_specific<phmap::flat_hash_map<tuple<long,VarChar<25>,VarChar<40>,long,VarChar<15>,double,VarChar<101>,VarChar<1>,long,VarChar<25>,long,VarChar<152>,VarChar<1>>,bool>> v1728;
auto v1669 = phmap::flat_hash_map<tuple<long,long,long,long,double,double,double,double,VarChar<1>,VarChar<1>,long,long,long,VarChar<25>,VarChar<10>,VarChar<44>,VarChar<1>>,bool>({});
tbb::enumerable_thread_specific<phmap::flat_hash_map<tuple<long,long,long,long,double,double,double,double,VarChar<1>,VarChar<1>,long,long,long,VarChar<25>,VarChar<10>,VarChar<44>,VarChar<1>>,bool>> v1735;
auto v1671 = phmap::flat_hash_map<tuple<long>,tuple<double>>({});
auto v1673 = phmap::flat_hash_map<tuple<long,double>,bool>({});
auto v1675 = phmap::flat_hash_map<tuple<long>,tuple<double>>({});
tbb::enumerable_thread_specific<phmap::flat_hash_map<tuple<long>,tuple<double>>> v1742;
auto v1677 = phmap::flat_hash_map<tuple<long,double>,bool>({});
auto v1679 = phmap::flat_hash_map<tuple<long,long,long,long,double,double,double,double,VarChar<1>,VarChar<1>,long,long,long,VarChar<25>,VarChar<10>,VarChar<44>,VarChar<1>>,bool>({});
tbb::enumerable_thread_specific<phmap::flat_hash_map<tuple<long,long,long,long,double,double,double,double,VarChar<1>,VarChar<1>,long,long,long,VarChar<25>,VarChar<10>,VarChar<44>,VarChar<1>>,bool>> v1749;
auto v1681 = phmap::flat_hash_map<long,phmap::flat_hash_map<tuple<long,double>,bool>>({});
auto v1685 = phmap::flat_hash_map<tuple<long,long,long,long,double,double,double,double,VarChar<1>,VarChar<1>,long,long,long,VarChar<25>,VarChar<10>,VarChar<44>,VarChar<1>,long,double>,bool>({});
auto v1687 = phmap::flat_hash_map<long,phmap::flat_hash_map<tuple<long,double>,bool>>({});
auto v1691 = phmap::flat_hash_map<tuple<long,long,long,long,double,double,double,double,VarChar<1>,VarChar<1>,long,long,long,VarChar<25>,VarChar<10>,VarChar<44>,VarChar<1>,long,double,long,double>,bool>({});
auto v1693 = phmap::flat_hash_map<long,phmap::flat_hash_map<tuple<long,VarChar<25>,VarChar<40>,long,VarChar<15>,double,VarChar<101>,VarChar<1>,long,VarChar<25>,long,VarChar<152>,VarChar<1>>,bool>>({});
auto v1697 = phmap::flat_hash_map<tuple<long,long,long,long,double,double,double,double,VarChar<1>,VarChar<1>,long,long,long,VarChar<25>,VarChar<10>,VarChar<44>,VarChar<1>,long,double,long,double,long,VarChar<25>,VarChar<40>,long,VarChar<15>,double,VarChar<101>,VarChar<1>,long,VarChar<25>,long,VarChar<152>,VarChar<1>>,bool>({});
auto v1699 = phmap::flat_hash_map<long,phmap::flat_hash_map<tuple<long,long,VarChar<1>,double,long,VarChar<15>,VarChar<15>,long,VarChar<79>,VarChar<1>>,bool>>({});
auto v1703 = phmap::flat_hash_map<tuple<long,long,long,long,double,double,double,double,VarChar<1>,VarChar<1>,long,long,long,VarChar<25>,VarChar<10>,VarChar<44>,VarChar<1>,long,double,long,double,long,VarChar<25>,VarChar<40>,long,VarChar<15>,double,VarChar<101>,VarChar<1>,long,VarChar<25>,long,VarChar<152>,VarChar<1>,long,long,VarChar<1>,double,long,VarChar<15>,VarChar<15>,long,VarChar<79>,VarChar<1>>,bool>({});
auto v1705 = phmap::flat_hash_map<tuple<long,long,long,long,double,double,double,double,VarChar<1>,VarChar<1>,long,long,long,VarChar<25>,VarChar<10>,VarChar<44>,VarChar<1>,long,double,long,double,long,VarChar<25>,VarChar<40>,long,VarChar<15>,double,VarChar<101>,VarChar<1>,long,VarChar<25>,long,VarChar<152>,VarChar<1>,long,long,VarChar<1>,double,long,VarChar<15>,VarChar<15>,long,VarChar<79>,VarChar<1>>,bool>({});
auto v1707 = phmap::flat_hash_map<tuple<VarChar<25>>,tuple<double>>({});
auto v1709 = phmap::flat_hash_map<tuple<VarChar<25>,double>,bool>({});	
	const auto& f = ConstantString("F", 2);const auto& saudiarabia = ConstantString("SAUDI ARABIA", 13);

auto v1710 = db->ord_dataset_size; tbb::parallel_for(tbb::blocked_range<size_t>(0, v1710), [&](const tbb::blocked_range<size_t>& v1711){auto& v1715=v1714.local();for (size_t v1658=v1711.begin(), end=v1711.end(); v1658!=end; ++v1658){if ((o_orderstatus[v1658] == f)){v1715[make_tuple(o_orderkey[v1658], o_custkey[v1658], o_orderstatus[v1658], o_totalprice[v1658], o_orderdate[v1658], o_orderpriority[v1658], o_clerk[v1658], o_shippriority[v1658], o_comment[v1658], o_NA[v1658])] += true;};}});for (auto& local : v1714)AddMap<phmap::flat_hash_map<tuple<long,long,VarChar<1>,double,long,VarChar<15>,VarChar<15>,long,VarChar<79>,VarChar<1>>,bool>,tuple<long,long,VarChar<1>,double,long,VarChar<15>,VarChar<15>,long,VarChar<79>,VarChar<1>>,bool>(v1659, local);const auto& orders_nation_supplier_l3_l2_lineitem_build_pre_ops = v1659;

auto v1717 = db->na_dataset_size; tbb::parallel_for(tbb::blocked_range<size_t>(0, v1717), [&](const tbb::blocked_range<size_t>& v1718){auto& v1722=v1721.local();for (size_t v1660=v1718.begin(), end=v1718.end(); v1660!=end; ++v1660){if ((n_name[v1660] == saudiarabia)){v1722[make_tuple(n_nationkey[v1660], n_name[v1660], n_regionkey[v1660], n_comment[v1660], n_NA[v1660])] += true;};}});for (auto& local : v1721)AddMap<phmap::flat_hash_map<tuple<long,VarChar<25>,long,VarChar<152>,VarChar<1>>,bool>,tuple<long,VarChar<25>,long,VarChar<152>,VarChar<1>>,bool>(v1661, local);const auto& nation_supplier_build_pre_ops = v1661;

for (auto& v1662 : nation_supplier_build_pre_ops){v1663[ /* n_nationkey */get<0>((v1662.first))] += phmap::flat_hash_map<tuple<long,VarChar<25>,long,VarChar<152>,VarChar<1>>,bool>({{(v1662.first), (v1662.second)}});}const auto& nation_supplier_build_nest_dict = v1663;

auto v1724 = db->su_dataset_size; tbb::parallel_for(tbb::blocked_range<size_t>(0, v1724), [&](const tbb::blocked_range<size_t>& v1725){auto& v1729=v1728.local();for (size_t v1664=v1725.begin(), end=v1725.end(); v1664!=end; ++v1664){if (((nation_supplier_build_nest_dict).contains(s_nationkey[v1664]))){for (auto& v1665 : (nation_supplier_build_nest_dict).at(s_nationkey[v1664])){v1729[tuple_cat(make_tuple(s_suppkey[v1664], s_name[v1664], s_address[v1664], s_nationkey[v1664], s_phone[v1664], s_acctbal[v1664], s_comment[v1664], s_NA[v1664]),move(make_tuple(n_nationkey[v1665], n_name[v1665], n_regionkey[v1665], n_comment[v1665], n_NA[v1665])))] += true;};};}});for (auto& local : v1728)AddMap<phmap::flat_hash_map<tuple<long,VarChar<25>,VarChar<40>,long,VarChar<15>,double,VarChar<101>,VarChar<1>,long,VarChar<25>,long,VarChar<152>,VarChar<1>>,bool>,tuple<long,VarChar<25>,VarChar<40>,long,VarChar<15>,double,VarChar<101>,VarChar<1>,long,VarChar<25>,long,VarChar<152>,VarChar<1>>,bool>(v1667, local);const auto& nation_supplier_l3_l2_lineitem_build_pre_ops = v1667;

auto v1731 = db->li_dataset_size; tbb::parallel_for(tbb::blocked_range<size_t>(0, v1731), [&](const tbb::blocked_range<size_t>& v1732){auto& v1736=v1735.local();for (size_t v1668=v1732.begin(), end=v1732.end(); v1668!=end; ++v1668){if ((l_receiptdate[v1668] > l_commitdate[v1668])){v1736[make_tuple(l_orderkey[v1668], l_partkey[v1668], l_suppkey[v1668], l_linenumber[v1668], l_quantity[v1668], l_extendedprice[v1668], l_discount[v1668], l_tax[v1668], l_returnflag[v1668], l_linestatus[v1668], l_shipdate[v1668], l_commitdate[v1668], l_receiptdate[v1668], l_shipinstruct[v1668], l_shipmode[v1668], l_comment[v1668], l_NA[v1668])] += true;};}});for (auto& local : v1735)AddMap<phmap::flat_hash_map<tuple<long,long,long,long,double,double,double,double,VarChar<1>,VarChar<1>,long,long,long,VarChar<25>,VarChar<10>,VarChar<44>,VarChar<1>>,bool>,tuple<long,long,long,long,double,double,double,double,VarChar<1>,VarChar<1>,long,long,long,VarChar<25>,VarChar<10>,VarChar<44>,VarChar<1>>,bool>(v1669, local);const auto& l3_0 = v1669;

for (auto& v1670 : l3_0){v1671[make_tuple( /* l_orderkey */get<0>((v1670.first)))] += make_tuple((( /* l_suppkey */get<2>((v1670.first)) != none)) ? (1.0) : (0.0));}const auto& l3_1 = v1671;

for (auto& v1672 : l3_1){v1673[tuple_cat((v1672.first),move((v1672.second)))] += true;}const auto& l3_l2_lineitem_build_pre_ops = v1673;

auto v1738 = db->li_dataset_size; tbb::parallel_for(tbb::blocked_range<size_t>(0, v1738), [&](const tbb::blocked_range<size_t>& v1739){auto& v1743=v1742.local();for (size_t v1674=v1739.begin(), end=v1739.end(); v1674!=end; ++v1674){v1743[make_tuple(l_orderkey[v1674])] += make_tuple(((l_suppkey[v1674] != none)) ? (1.0) : (0.0));}});for (auto& local : v1742)AddMap<phmap::flat_hash_map<tuple<long>,tuple<double>>,tuple<long>,tuple<double>>(v1675, local);const auto& l2_0 = v1675;

for (auto& v1676 : l2_0){v1677[tuple_cat((v1676.first),move((v1676.second)))] += true;}const auto& l2_lineitem_build_pre_ops = v1677;

auto v1745 = db->li_dataset_size; tbb::parallel_for(tbb::blocked_range<size_t>(0, v1745), [&](const tbb::blocked_range<size_t>& v1746){auto& v1750=v1749.local();for (size_t v1678=v1746.begin(), end=v1746.end(); v1678!=end; ++v1678){if ((l_receiptdate[v1678] > l_commitdate[v1678])){v1750[make_tuple(l_orderkey[v1678], l_partkey[v1678], l_suppkey[v1678], l_linenumber[v1678], l_quantity[v1678], l_extendedprice[v1678], l_discount[v1678], l_tax[v1678], l_returnflag[v1678], l_linestatus[v1678], l_shipdate[v1678], l_commitdate[v1678], l_receiptdate[v1678], l_shipinstruct[v1678], l_shipmode[v1678], l_comment[v1678], l_NA[v1678])] += true;};}});for (auto& local : v1749)AddMap<phmap::flat_hash_map<tuple<long,long,long,long,double,double,double,double,VarChar<1>,VarChar<1>,long,long,long,VarChar<25>,VarChar<10>,VarChar<44>,VarChar<1>>,bool>,tuple<long,long,long,long,double,double,double,double,VarChar<1>,VarChar<1>,long,long,long,VarChar<25>,VarChar<10>,VarChar<44>,VarChar<1>>,bool>(v1679, local);const auto& l2_lineitem_probe_pre_ops = v1679;

for (auto& v1680 : l2_lineitem_build_pre_ops){v1681[ /* l_orderkey */get<0>((v1680.first))] += phmap::flat_hash_map<tuple<long,double>,bool>({{(v1680.first), (v1680.second)}});}const auto& l2_lineitem_build_nest_dict = v1681;

for (auto& v1682 : l2_lineitem_probe_pre_ops){if (((l2_lineitem_build_nest_dict).contains( /* l_orderkey */get<0>((v1682.first))))){for (auto& v1683 : (l2_lineitem_build_nest_dict).at( /* l_orderkey */get<0>((v1682.first)))){v1685[tuple_cat((v1682.first),move((v1683.first)))] += true;};};}const auto& l3_l2_lineitem_probe_pre_ops = v1685;

for (auto& v1686 : l3_l2_lineitem_build_pre_ops){v1687[ /* l_orderkey */get<0>((v1686.first))] += phmap::flat_hash_map<tuple<long,double>,bool>({{(v1686.first), (v1686.second)}});}const auto& l3_l2_lineitem_build_nest_dict = v1687;

for (auto& v1688 : l3_l2_lineitem_probe_pre_ops){if (((l3_l2_lineitem_build_nest_dict).contains( /* l_orderkey */get<0>((v1688.first))))){for (auto& v1689 : (l3_l2_lineitem_build_nest_dict).at( /* l_orderkey */get<0>((v1688.first)))){v1691[tuple_cat((v1688.first),move((v1689.first)))] += true;};};}const auto& nation_supplier_l3_l2_lineitem_probe_pre_ops = v1691;

for (auto& v1692 : nation_supplier_l3_l2_lineitem_build_pre_ops){v1693[ /* s_suppkey */get<0>((v1692.first))] += phmap::flat_hash_map<tuple<long,VarChar<25>,VarChar<40>,long,VarChar<15>,double,VarChar<101>,VarChar<1>,long,VarChar<25>,long,VarChar<152>,VarChar<1>>,bool>({{(v1692.first), (v1692.second)}});}const auto& nation_supplier_l3_l2_lineitem_build_nest_dict = v1693;

for (auto& v1694 : nation_supplier_l3_l2_lineitem_probe_pre_ops){if (((nation_supplier_l3_l2_lineitem_build_nest_dict).contains( /* l_suppkey */get<2>((v1694.first))))){for (auto& v1695 : (nation_supplier_l3_l2_lineitem_build_nest_dict).at( /* l_suppkey */get<2>((v1694.first)))){v1697[tuple_cat((v1694.first),move((v1695.first)))] += true;};};}const auto& orders_nation_supplier_l3_l2_lineitem_probe_pre_ops = v1697;

for (auto& v1698 : orders_nation_supplier_l3_l2_lineitem_build_pre_ops){v1699[ /* o_orderkey */get<0>((v1698.first))] += phmap::flat_hash_map<tuple<long,long,VarChar<1>,double,long,VarChar<15>,VarChar<15>,long,VarChar<79>,VarChar<1>>,bool>({{(v1698.first), (v1698.second)}});}const auto& orders_nation_supplier_l3_l2_lineitem_build_nest_dict = v1699;

for (auto& v1700 : orders_nation_supplier_l3_l2_lineitem_probe_pre_ops){if (((orders_nation_supplier_l3_l2_lineitem_build_nest_dict).contains( /* l_orderkey */get<0>((v1700.first))))){for (auto& v1701 : (orders_nation_supplier_l3_l2_lineitem_build_nest_dict).at( /* l_orderkey */get<0>((v1700.first)))){v1703[tuple_cat((v1700.first),move((v1701.first)))] += true;};};}const auto& orders_nation_supplier_l3_l2_lineitem_0 = v1703;

for (auto& v1704 : orders_nation_supplier_l3_l2_lineitem_0){if ((( /* l2_size */get<18>((v1704.first)) > (long)1) && ( /* l3_size */get<20>((v1704.first)) == (long)1))){v1705[(v1704.first)] += (v1704.second);};}const auto& orders_nation_supplier_l3_l2_lineitem_1 = v1705;

for (auto& v1706 : orders_nation_supplier_l3_l2_lineitem_1){v1707[make_tuple( /* s_name */get<22>((v1706.first)))] += make_tuple((( /* s_name */get<22>((v1706.first)) != none)) ? (1.0) : (0.0));}const auto& orders_nation_supplier_l3_l2_lineitem_2 = v1707;

for (auto& v1708 : orders_nation_supplier_l3_l2_lineitem_2){v1709[tuple_cat((v1708.first),move((v1708.second)))] += true;}const auto& results = v1709;const auto& out = results;

    FastDict_s25f_b* result = (FastDict_s25f_b*)PyObject_CallObject(PyObject_CallObject(PyObject_GetAttrString(PyImport_Import(PyUnicode_FromString("test_unopt_fastdict_compiled")), (char*)"new_FastDict_s25f_b"), nullptr), nullptr);
    *(result->dict) = out;

    return (PyObject*)PyObject_CallObject(PyObject_GetAttrString(PyImport_Import(PyUnicode_FromString("sdqlpy.fastd")), "fastd"), Py_BuildValue("(OO)", result, PyUnicode_FromString("test_unopt")));

}

static PyObject * q22(PyObject *self, PyObject* args)
{
	

    PyObject * db_;
    if (!PyArg_ParseTuple(args, "O", &db_)){
        PyErr_SetString(PyExc_ValueError,"Error while parsing the sum inputs.");
        return NULL;
    }
    
    
    DB dbobj;
    DB* db = &dbobj;
    
const static int numpy_initialized =  init_numpy();


	auto cu_size = PyArray_Size(PyList_GetItem(PyList_GetItem(db_, 0), 0));
	db->cu_dataset_size = cu_size;
	auto c_custkey = (long*)PyArray_DATA(PyList_GetItem(PyList_GetItem(db_, 0), 0));
	auto c_name= (VarChar<25>*)PyArray_DATA(PyList_GetItem(PyList_GetItem(db_, 0), 1));
	auto c_address= (VarChar<40>*)PyArray_DATA(PyList_GetItem(PyList_GetItem(db_, 0), 2));
	auto c_nationkey = (long*)PyArray_DATA(PyList_GetItem(PyList_GetItem(db_, 0), 3));
	auto c_phone= (VarChar<15>*)PyArray_DATA(PyList_GetItem(PyList_GetItem(db_, 0), 4));
	auto c_acctbal = (double*)PyArray_DATA(PyList_GetItem(PyList_GetItem(db_, 0), 5));
	auto c_mktsegment= (VarChar<10>*)PyArray_DATA(PyList_GetItem(PyList_GetItem(db_, 0), 6));
	auto c_comment= (VarChar<117>*)PyArray_DATA(PyList_GetItem(PyList_GetItem(db_, 0), 7));
	auto c_NA= (VarChar<1>*)PyArray_DATA(PyList_GetItem(PyList_GetItem(db_, 0), 8));

	auto ord_size = PyArray_Size(PyList_GetItem(PyList_GetItem(db_, 1), 0));
	db->ord_dataset_size = ord_size;
	auto o_orderkey = (long*)PyArray_DATA(PyList_GetItem(PyList_GetItem(db_, 1), 0));
	auto o_custkey = (long*)PyArray_DATA(PyList_GetItem(PyList_GetItem(db_, 1), 1));
	auto o_orderstatus= (VarChar<1>*)PyArray_DATA(PyList_GetItem(PyList_GetItem(db_, 1), 2));
	auto o_totalprice = (double*)PyArray_DATA(PyList_GetItem(PyList_GetItem(db_, 1), 3));
	auto o_orderdate = (long*)PyArray_DATA(PyList_GetItem(PyList_GetItem(db_, 1), 4));
	auto o_orderpriority= (VarChar<15>*)PyArray_DATA(PyList_GetItem(PyList_GetItem(db_, 1), 5));
	auto o_clerk= (VarChar<15>*)PyArray_DATA(PyList_GetItem(PyList_GetItem(db_, 1), 6));
	auto o_shippriority = (long*)PyArray_DATA(PyList_GetItem(PyList_GetItem(db_, 1), 7));
	auto o_comment= (VarChar<79>*)PyArray_DATA(PyList_GetItem(PyList_GetItem(db_, 1), 8));
	auto o_NA= (VarChar<1>*)PyArray_DATA(PyList_GetItem(PyList_GetItem(db_, 1), 9));



	
auto v1769 = phmap::flat_hash_map<tuple<long,VarChar<25>,VarChar<40>,long,VarChar<15>,double,VarChar<10>,VarChar<117>,VarChar<1>>,bool>({});
tbb::enumerable_thread_specific<phmap::flat_hash_map<tuple<long,VarChar<25>,VarChar<40>,long,VarChar<15>,double,VarChar<10>,VarChar<117>,VarChar<1>>,bool>> v1788;
auto v1771 = make_tuple(0.0,0.0);
auto v1773 = phmap::flat_hash_map<tuple<long,VarChar<25>,VarChar<40>,long,VarChar<15>,double,VarChar<10>,VarChar<117>,VarChar<1>>,bool>({});
tbb::enumerable_thread_specific<phmap::flat_hash_map<tuple<long,VarChar<25>,VarChar<40>,long,VarChar<15>,double,VarChar<10>,VarChar<117>,VarChar<1>>,bool>> v1795;
auto v1775 = phmap::flat_hash_map<long,bool>({});
tbb::enumerable_thread_specific<phmap::flat_hash_map<long,bool>> v1802;
auto v1777 = phmap::flat_hash_map<tuple<long,VarChar<25>,VarChar<40>,long,VarChar<15>,double,VarChar<10>,VarChar<117>,VarChar<1>>,bool>({});
auto v1779 = phmap::flat_hash_map<tuple<long,VarChar<25>,VarChar<40>,long,VarChar<15>,double,VarChar<10>,VarChar<117>,VarChar<1>,VarChar<2>>,bool>({});
auto v1781 = phmap::flat_hash_map<tuple<VarChar<2>>,tuple<double,double>>({});
auto v1783 = phmap::flat_hash_map<tuple<VarChar<2>,double,double>,bool>({});	
	const auto& v13 = ConstantString("13", 3);const auto& v31 = ConstantString("31", 3);const auto& v23 = ConstantString("23", 3);const auto& v29 = ConstantString("29", 3);const auto& v30 = ConstantString("30", 3);const auto& v18 = ConstantString("18", 3);const auto& v17 = ConstantString("17", 3);

auto v1784 = db->cu_dataset_size; tbb::parallel_for(tbb::blocked_range<size_t>(0, v1784), [&](const tbb::blocked_range<size_t>& v1785){auto& v1789=v1788.local();for (size_t v1768=v1785.begin(), end=v1785.end(); v1768!=end; ++v1768){if (((c_acctbal[v1768] > 0.0) && (((((((c_phone[v1768].startsWith(v13)) || (c_phone[v1768].startsWith(v31))) || (c_phone[v1768].startsWith(v23))) || (c_phone[v1768].startsWith(v29))) || (c_phone[v1768].startsWith(v30))) || (c_phone[v1768].startsWith(v18))) || (c_phone[v1768].startsWith(v17))))){v1789[make_tuple(c_custkey[v1768], c_name[v1768], c_address[v1768], c_nationkey[v1768], c_phone[v1768], c_acctbal[v1768], c_mktsegment[v1768], c_comment[v1768], c_NA[v1768])] += true;};}});for (auto& local : v1788)AddMap<phmap::flat_hash_map<tuple<long,VarChar<25>,VarChar<40>,long,VarChar<15>,double,VarChar<10>,VarChar<117>,VarChar<1>>,bool>,tuple<long,VarChar<25>,VarChar<40>,long,VarChar<15>,double,VarChar<10>,VarChar<117>,VarChar<1>>,bool>(v1769, local);const auto& cu1_0 = v1769;

for (auto& v1770 : cu1_0){make_tuple( /* c_acctbal */get<5>((v1770.first)),1.0);}const auto& JQ_sum_acctbal_div_count_acctbal_XZ_pre_ops = v1771;

auto v1791 = db->cu_dataset_size; tbb::parallel_for(tbb::blocked_range<size_t>(0, v1791), [&](const tbb::blocked_range<size_t>& v1792){auto& v1796=v1795.local();for (size_t v1772=v1792.begin(), end=v1792.end(); v1772!=end; ++v1772){if (((c_acctbal[v1772] > (sum_acctbal[v1772] / count_acctbal[v1772])) && (((((((c_phone[v1772].startsWith(v13)) || (c_phone[v1772].startsWith(v31))) || (c_phone[v1772].startsWith(v23))) || (c_phone[v1772].startsWith(v29))) || (c_phone[v1772].startsWith(v30))) || (c_phone[v1772].startsWith(v18))) || (c_phone[v1772].startsWith(v17))))){v1796[make_tuple(c_custkey[v1772], c_name[v1772], c_address[v1772], c_nationkey[v1772], c_phone[v1772], c_acctbal[v1772], c_mktsegment[v1772], c_comment[v1772], c_NA[v1772])] += true;};}});for (auto& local : v1795)AddMap<phmap::flat_hash_map<tuple<long,VarChar<25>,VarChar<40>,long,VarChar<15>,double,VarChar<10>,VarChar<117>,VarChar<1>>,bool>,tuple<long,VarChar<25>,VarChar<40>,long,VarChar<15>,double,VarChar<10>,VarChar<117>,VarChar<1>>,bool>(v1773, local);const auto& customer_0 = v1773;

auto v1798 = db->ord_dataset_size; tbb::parallel_for(tbb::blocked_range<size_t>(0, v1798), [&](const tbb::blocked_range<size_t>& v1799){auto& v1803=v1802.local();for (size_t v1774=v1799.begin(), end=v1799.end(); v1774!=end; ++v1774){v1803[o_custkey[v1774]] += true;}});for (auto& local : v1802)AddMap<phmap::flat_hash_map<long,bool>,long,bool>(v1775, local);const auto& orders_customer_isin_build_index = v1775;

for (auto& v1776 : customer_0){if ((!((orders_customer_isin_build_index).contains( /* c_custkey */get<0>((v1776.first)))))){v1777[(v1776.first)] += (v1776.second);};}const auto& customer_1 = v1777;

for (auto& v1778 : customer_1){v1779[tuple_cat((v1778.first),move(make_tuple( /* c_phone */get<4>((v1778.first)).substr<2>((long)0,(long)1))))] += (v1778.second);}const auto& customer_2 = v1779;

for (auto& v1780 : customer_2){v1781[make_tuple( /* cntrycode */get<9>((v1780.first)))] += make_tuple((( /* c_acctbal */get<5>((v1780.first)) != none)) ? (1.0) : (0.0), /* c_acctbal */get<5>((v1780.first)));}const auto& customer_3 = v1781;

for (auto& v1782 : customer_3){v1783[tuple_cat((v1782.first),move((v1782.second)))] += true;}const auto& results = v1783;const auto& out = results;

    FastDict_s2ff_b* result = (FastDict_s2ff_b*)PyObject_CallObject(PyObject_CallObject(PyObject_GetAttrString(PyImport_Import(PyUnicode_FromString("test_unopt_fastdict_compiled")), (char*)"new_FastDict_s2ff_b"), nullptr), nullptr);
    *(result->dict) = out;

    return (PyObject*)PyObject_CallObject(PyObject_GetAttrString(PyImport_Import(PyUnicode_FromString("sdqlpy.fastd")), "fastd"), Py_BuildValue("(OO)", result, PyUnicode_FromString("test_unopt")));

}


static PyMethodDef test_unopt_compiled_methods[] = {
{"q1_compiled", q1, METH_VARARGS, ""},
{"q2_compiled", q2, METH_VARARGS, ""},
{"q3_compiled", q3, METH_VARARGS, ""},
{"q4_compiled", q4, METH_VARARGS, ""},
{"q5_compiled", q5, METH_VARARGS, ""},
{"q6_compiled", q6, METH_VARARGS, ""},
{"q7_compiled", q7, METH_VARARGS, ""},
{"q8_compiled", q8, METH_VARARGS, ""},
{"q9_compiled", q9, METH_VARARGS, ""},
{"q10_compiled", q10, METH_VARARGS, ""},
{"q11_compiled", q11, METH_VARARGS, ""},
{"q12_compiled", q12, METH_VARARGS, ""},
{"q14_compiled", q14, METH_VARARGS, ""},
{"q15_compiled", q15, METH_VARARGS, ""},
{"q16_compiled", q16, METH_VARARGS, ""},
{"q17_compiled", q17, METH_VARARGS, ""},
{"q18_compiled", q18, METH_VARARGS, ""},
{"q19_compiled", q19, METH_VARARGS, ""},
{"q20_compiled", q20, METH_VARARGS, ""},
{"q21_compiled", q21, METH_VARARGS, ""},
{"q22_compiled", q22, METH_VARARGS, ""},

{NULL,		NULL}		/* sentinel */
};

///////////////////////////////////////////////////////////////////////

static char module_docstring[] = "";

static struct PyModuleDef test_unopt_compiled = 
{
    PyModuleDef_HEAD_INIT,
    "test_unopt_compiled",
    module_docstring,
    -1,
    test_unopt_compiled_methods
};

PyMODINIT_FUNC PyInit_test_unopt_compiled(void) 
{
    return PyModule_Create(&test_unopt_compiled);
}

int main(int argc, char **argv)
{
	Py_SetProgramName((wchar_t*)argv[0]);
	Py_Initialize();
	PyInit_test_unopt_compiled();
	Py_Exit(0);
}
