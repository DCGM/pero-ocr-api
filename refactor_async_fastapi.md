## Goal 
Refactor pero-ocr-api to use FastAPI, fully async code including filesystem and database access and up to date versions of packages. Other enhancements should be:
- Use alembic for database versioning. Prepare it for the current database and prepare a way how to "migrate" an existing databases to a alembic versioned databases.
- Extract the database model definition to a separate package outside `./app` such the it would be independed on the API backend.
- Add pydantic models to define the API parameters and return values.
- Update config to accept also environment variables.
- Update tests to support the updated async database connections and use the tests to verify the refactored version.
- Remove all unused dependencies
- Add dependencies if needed.
- Update readme as need. 
- Add migration.md file explaining how to migrate to the new verions - expecially how to make current databases support the alembic. 

## Packages 
- fastapi
- SQLAlchemy[asyncio]
- psycopg2-binary
- alembic
- pydantic
- uvicorn
- aiopath
- aiofiles

## Code guidelines
- Setup logging properly using the `logging` module - create logger or loggers as appropriate.


### Database model style example
```
class Annotation(Base):
    __tablename__ = 'annotations'
    id: Mapped[uuid.UUID] = mapped_column(primary_key=True, default=uuid.uuid4)
    text_original: Mapped[str]
    text_edited: Mapped[str]
    character_change_count: Mapped[int] = mapped_column(default=init_character_change_count)
    character_count: Mapped[int] = mapped_column(default=init_character_count)

    created_date: Mapped[datetime] = mapped_column(default=datetime.utcnow, index=True)
    text_line_id: Mapped[uuid.UUID] = mapped_column(ForeignKey('text_lines.id'), index=True)
    user_id: Mapped[uuid.UUID] = mapped_column(ForeignKey('users.id'), index=True)

    text_line: Mapped['TextLine'] = relationship(viewonly=True)
    user: Mapped['User'] = relationship(viewonly=True)
```

### Database connection and session style
```
async def get_async_session() -> AsyncGenerator[AsyncSession, None]:
    global global_engine, global_async_session_maker
    if global_engine is None:
        global_engine = create_async_engine(
            config.DATABASE_URL,
            pool_size=config.DATABASE_POOL_SIZE,
            max_overflow=config.DATABASE_MAX_OVERFLOW,
            pool_timeout=config.DATABASE_POOL_TIMEOUT,
            pool_recycle=config.DATABASE_POOL_RECYCLE,
            pool_pre_ping=True,
        )
        global_async_session_maker = async_sessionmaker(global_engine,
                                                        expire_on_commit=False,
                                                        autocommit=False,
                                                        autoflush=False)
    async with global_async_session_maker() as session:
        yield session
```


### Route style
User `response_model`, `tags` and possibly add API endpoint explanation string to the automaticaly generated API documentation.
Use `Depends` to check authentication of USER or SUPER_USER and. Use `Depends` to get AsyncSession.
Use separate `guard` functions which chech access rights where appropriate.
Separate database access to separate functions. 
Raise custom expections as appropriate and handle them using the "global" FastAPI mechanism.
```
@job_route.get("/document/{document_id}", response_model=List[base_objects.ProcessingJob], tags=["Processing Job"])
async def get_processing_jobs_for_document(document_id: UUID,
                                           user_token: TokenUser = Depends(get_user_info), db: AsyncSession = Depends(get_async_session)):
    await route_guards.challenge_user_access_to_document_with_forbidden_message(db, user_token, document_id)
    return await crud_job.get_processing_jobs_for_documents(db, [document_id])
```
