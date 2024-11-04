import {
  connectToDatabase,
  closeDatabaseConnection,
  db,
} from "./dbConnection.js";

const collectionName = "video_movieDetails";
const videoMovieDetailsCollection = db.collection(collectionName);
const moviesCollection = db.collection("video_movies");

// query part a (pipeline)
const getDirectorMovieCount = async () => {
  try {
    const pipeline = [
      {
        $addFields: {
          directorsArray: { $split: ["$director", ", "] },
        },
      },
      { $unwind: "$directorsArray" },
      {
        $addFields: {
          directorsArray: { $trim: { input: "$directorsArray" } },
        },
      },
      {
        $group: {
          _id: "$directorsArray",
          movieCount: { $sum: 1 },
        },
      },
      {
        $project: {
          _id: 0,
          director: "$_id",
          movieCount: 1,
        },
      },
      { $sort: { movieCount: -1 } },

      // filtering by director name
      // { $match: { director: directorName } }
    ];

    const result = await videoMovieDetailsCollection
      .aggregate(pipeline)
      .toArray();
    return result;
  } catch (error) {
    console.error("Error retrieving director movie counts:", error);
  }
};

//query part B
const updateDirectorName = async (currentDirectorName, newDirectorName) => {
  try {
    const filter = { director: currentDirectorName };

    const update = { $set: { director: newDirectorName } };

    const result = await videoMovieDetailsCollection.updateMany(filter, update);

    return result;
  } catch (error) {
    console.error("Error retrieving director movie counts:", error);
  }
};

//query part D
const getMoviesFilmedInCountry = async (country) => {
  try {
    const pipeline = [
      {
        $lookup: {
          from: "video_movieDetails",
          localField: "imdb",
          foreignField: "imdb.id",
          as: "movieDetails",
        }
      },
      {
        $match: {
          "movieDetails.countries": country 
        }
      },
      {
        $project: {
          _id: 0,
          title: 1
        }
      }
    ];

    const result = await moviesCollection.aggregate(pipeline).toArray();

    return result;
  } catch (error) {
    console.error(`Error retrieving movies filmed in ${country}:`, error);
  }
};

const ExerciseTwoPartA = async () => {
  console.log("Exercise Two Part A");
  const result = await getDirectorMovieCount();
  result && console.log(`name and number of movies of each director`, result);

  console.log(
    "\n ****************************************************************************************** \n"
  );
};

const ExerciseTwoPartB = async () => {
  console.log("Exercise Two Part B");
  const currentDirectorName = "Roland Emmerich";
  const newDirectorName = "R. Emmerich";

  const result = await updateDirectorName(newDirectorName, currentDirectorName);

  console.log(`Updated ${result.modifiedCount} movies.`);

  console.log(
    "\n ****************************************************************************************** \n"
  );
};

// query part C
const ExerciseTwoPartC = async () => {
  console.log("Exercise Two Part C");
  const countryName = "France";

  const result = await getMoviesFilmedInCountry(countryName);

  console.log(`Movies filmed in ${countryName}:`, result);

  console.log(
    "\n ****************************************************************************************** \n"
  );
};

const ExerciseTwo = async () => {
  await connectToDatabase();

  await ExerciseTwoPartA();
  await ExerciseTwoPartB();
  await ExerciseTwoPartC();

  await closeDatabaseConnection();
};

ExerciseTwo();
