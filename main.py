from fastapi import FastAPI, HTTPException, Query
from pydantic import BaseModel
from bson import ObjectId
from apscheduler.schedulers.background import BackgroundScheduler
import pandas as pd
import requests
import io
from urllib.parse import quote_plus
from contextlib import asynccontextmanager
from typing import Optional
from pymongo.mongo_client import MongoClient
from pymongo.server_api import ServerApi
from fastapi.middleware.cors import CORSMiddleware
from starlette.middleware import Middleware
from starlette.middleware.cors import CORSMiddleware
app = FastAPI()

middleware = [
    Middleware(
        CORSMiddleware,
        allow_origins=['*'],
        allow_credentials=True,
        allow_methods=['*'],
        allow_headers=['*']
    )
]
# Define the username and password
username = 'univ'  # Replace with your actual username
password = 'univ@123'  # Replace with your actual password

# Escape the username and password according to RFC 3986
escaped_username = quote_plus(username)
escaped_password = quote_plus(password)
# MongoDB connection
uri = f"mongodb+srv://{escaped_username}:{escaped_password}@univ.tx52a.mongodb.net/?retryWrites=true&w=majority&appName=univ"
# Create a new client and connect to the server
client = MongoClient(uri, server_api=ServerApi('1'))

db = client.course_database
course_collection = db.courses

# Helper function to convert MongoDB ObjectId to string


# Function to wipe out collection and insert new data from a CSV
def fetch_and_update_courses():
    try:
        # Fetch the CSV from an API (replace with the actual API URL)
        csv_url = "https://api.mockaroo.com/api/501b2790?count=100&key=8683a1c0"  # Replace with actual URL
        response = requests.get(csv_url)

        # Read the CSV content
        if response.status_code == 200:
            csv_data = response.content.decode('utf-8')
            df = pd.read_csv(io.StringIO(csv_data))

            # Wipe out the existing courses collection
            client.course_database.courses.delete_many({})

            # Prepare the list of courses to insert
            courses_to_insert = []
            for index, row in df.iterrows():
                course = {
                    "university": row['University'],
                    "city": row['City'],
                    "country": row['Country'],
                    "courseName": row['CourseName'],
                    "courseDescription": row['CourseDescription'],
                    "startDate": row['StartDate'],
                    "endDate": row['EndDate'],
                    "price": row['Price'],
                    "currency": row['Currency']
                }
                courses_to_insert.append(course)

            # Insert new courses into MongoDB
            if courses_to_insert:
                client.course_database.courses.insert_many(courses_to_insert)
                print(f"Inserted {len(courses_to_insert)} new courses.")
            else:
                print("No courses to insert.")
        else:
            print("Failed to fetch the CSV file.")
    except Exception as e:
        print(f"Error while fetching or inserting data: {e}")

def ping():
    print("ping")
# Scheduler setup

# Use async lifespan context to manage startup and shutdown
@asynccontextmanager
async def lifespan(app: FastAPI):
    # Startup: Start the scheduler
    # fetch_and_update_courses()
    scheduler = BackgroundScheduler()

    scheduler.add_job(fetch_and_update_courses, 'interval', minutes=1)
    scheduler.start()
    print("Scheduler started.")
    
    # Keep the app running
    yield
    
    # Shutdown: Stop the scheduler
    scheduler.shutdown()
    print("Scheduler shutdown.")

# Attach lifespan event handler
app = FastAPI(lifespan=lifespan, middleware=middleware)


def course_helper(course) -> dict:
    return {
        "id": str(course["_id"]),
        "university": course["university"],
        "city": course["city"],
        "country": course["country"],
        "courseName": course["courseName"],
        "courseDescription": course["courseDescription"],
        "startDate": course["startDate"],
        "endDate": course["endDate"],
        "price": course["price"],
        "currency": course["currency"]
    }

# Pydantic model for a course
class Course(BaseModel):
    university: str
    city: str
    country: str
    courseName: str
    courseDescription: str
    startDate: str
    endDate: str
    price: float
    currency: str
    _id: str

@app.post("/courses/", response_description="Add new course", response_model=Course)
async def create_course(course: Course):
    course_dict = course.model_dump()
    result = course_collection.insert_one(course_dict)
    created_course = course_collection.find_one({"_id": result.inserted_id})
    return course_helper(created_course)

@app.get("/courses/", response_description="List all courses with search and pagination")
async def get_courses(
    search: Optional[str] = None, 
    page: int = Query(1, ge=1), 
    limit: int = Query(10, ge=1, le=100)
):
    skip = (page - 1) * limit
    query = {}
    regexObj = {"$regex": search, "$options": "i"}
    # Search functionality (if search parameter is provided)
    if search:
        query = {
            "$or": [
        #         "id": str(course["_id"]),
        # "university": course["university"],
        # "city": course["city"],
        # "country": course["country"],
        # "courseName": course["courseName"],
        # "courseDescription": course["courseDescription"],
        # "startDate": course["startDate"],
        # "endDate": course["endDate"],
        # "price": course["price"],
        # "currency": course["currency"]
                {"university" : regexObj},
                {"city": regexObj},
                
            ]
        }
    
    courses = []
    courses_in = list(course_collection.find(query).skip(skip).limit(limit))
    courses = [course_helper(course) for course in courses_in]

    total_courses = course_collection.count_documents(query)
    return {
        "total": total_courses,
        "page": page,
        "limit": limit,
        "courses": courses
    }

@app.get("/courses/{id}", response_description="Get a single course", response_model=Course)
async def get_course(id: str):
    if not ObjectId.is_valid(id):
        raise HTTPException(status_code=400, detail="Invalid course ID")
    
    course = course_collection.find_one({"_id": ObjectId(id)})
    if course:
        return course_helper(course)
    raise HTTPException(status_code=404, detail="Course not found")

@app.put("/courses/{id}", response_description="Update a course", response_model=Course)
async def update_course(id: str, course: Course):
    if not ObjectId.is_valid(id):
        raise HTTPException(status_code=400, detail="Invalid course ID")

    # Prepare the course data to update, excluding fields with None values
    course_dict = {k: v for k, v in course.model_dump().items() if v is not None}

    # Perform the update operation in MongoDB
    update_result = course_collection.update_one(
        {"_id": ObjectId(id)},  # Find the course by its _id
        {"$set": course_dict}    # Set the new values for the course
    )

    # Check if the update was successful
    if update_result.matched_count == 0:
        raise HTTPException(status_code=404, detail="Course not found")

    # Return the updated course from MongoDB
    updated_course = course_collection.find_one({"_id": ObjectId(id)})
    
    if updated_course:
        return course_helper(updated_course)
    else:
        raise HTTPException(status_code=404, detail="Course not found")

@app.delete("/courses/{id}", response_description="Delete a course")
async def delete_course(id: str):
    delete_result = course_collection.delete_one({"_id": ObjectId(id)})
    if delete_result.deleted_count == 1:
        return {"message": "Course deleted successfully"}
    raise HTTPException(status_code=404, detail="Course not found")
