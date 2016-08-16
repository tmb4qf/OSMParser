using namespace std;

#define PI 3.14159265358979323846
#define R  6371

//user-defined types
typedef int64_t nodeId_t;
typedef float coord_t;
typedef double dist_t;

//node structure
typedef struct
{
	nodeId_t id;
	coord_t lat;
	coord_t lon;
	vector< pair<nodeId_t, dist_t> > adj;
} node_t;


//function prototypes
bool processDataBlock( map< nodeId_t, node_t > &fullData, OSMPBF::Blob* );
bool processHeaderBlock( OSMPBF::Blob* );
void inflateData( OSMPBF::Blob* b, unsigned char* test );
void processDenseNodes( map< nodeId_t, node_t > &fullData, OSMPBF::PrimitiveBlock* pb, int i );
void processWays( map< nodeId_t, node_t > &fullData, OSMPBF::PrimitiveBlock* pb, int i );
void postProcessNodes( map< nodeId_t, node_t > &fullData, map< nodeId_t, node_t > &reducedData );
dist_t calculateDist( coord_t lat1, coord_t lon1, coord_t lat2, coord_t lon2 );
double toRad( coord_t deg );
bool bulkWriteToDB( map< nodeId_t, node_t > &reducedData );