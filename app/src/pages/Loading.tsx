

export const Loading = () => {

    return (
        <div className="flex items-center justify-center min-h-screen">
            <div className="text-center">
                <div className="relative inline-flex">
                    <div className="w-16 h-16 border-4 border-light-primary/70 border-t-dark-primary rounded-full animate-spin"></div>
                    <div className="absolute inset-0 w-16 h-16 border-4 border-transparent border-r-dark-primary rounded-full animate-spin" style={{ animationDirection: 'reverse', animationDuration: '1s' }}></div>
                </div>
            </div>
        </div>
    )
}