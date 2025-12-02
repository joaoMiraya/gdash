import { ToggleThemeButton } from "../utils/ToggleThemeButton"


export const Container = ({ children }: { children: React.ReactNode }) => {

    return (
        <div className="relative min-h-screen bg-light-primary
            dark:bg-dark-primary dark:text-light-primary text-dark-primary"
        >
            <ToggleThemeButton />
            {children}
        </div>
    )
}